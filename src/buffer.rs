use core::{cmp, fmt, ops::Deref};

use embedded_io::{SliceWriteError, Write};
use mqttrs::{encode_slice, Packet};

use crate::Error;

/// A stack allocated buffer that can be written to and then read back from.
/// Dereferencing as a [`u8`] slice allows access to previously written data.
///
/// Can be written to with [`write!`] and supports [`embedded_io::Write`] and
/// [`embedded_io_async::Write`].
pub struct Buffer<const N: usize> {
    bytes: [u8; N],
    cursor: usize,
}

impl<const N: usize> Default for Buffer<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Buffer<N> {
    /// Creates a new buffer.
    pub(crate) const fn new() -> Self {
        Self {
            bytes: [0; N],
            cursor: 0,
        }
    }

    /// Creates a new buffer and writes the given data into it.
    pub(crate) fn from(buf: &[u8]) -> Result<Self, Error> {
        let mut buffer = Self::new();
        match buffer.write_all(buf) {
            Ok(()) => Ok(buffer),
            Err(_) => Err(Error::TooLarge),
        }
    }

    pub(crate) fn encode_packet(&mut self, packet: &Packet<'_>) -> Result<(), mqttrs::Error> {
        let len = encode_slice(packet, &mut self.bytes[self.cursor..])?;
        self.cursor += len;

        Ok(())
    }

    #[cfg(feature = "serde")]
    /// Serializes a value into this buffer using JSON.
    pub(crate) fn serialize_json<T: serde::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), serde_json_core::ser::Error> {
        let len = serde_json_core::to_slice(value, &mut self.bytes[self.cursor..])?;
        self.cursor += len;

        Ok(())
    }

    #[cfg(feature = "serde")]
    /// Deserializes this buffer using JSON into the given type.
    pub fn deserialize_json<'a, T: serde::Deserialize<'a>>(
        &'a self,
    ) -> Result<T, serde_json_core::de::Error> {
        let (result, _) = serde_json_core::from_slice(self)?;

        Ok(result)
    }

    /// The number of bytes available for writing into this buffer.
    pub fn available(&self) -> usize {
        N - self.cursor
    }
}

impl<const N: usize> Deref for Buffer<N> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes[0..self.cursor]
    }
}

impl<const N: usize> fmt::Write for Buffer<N> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.write_all(s.as_bytes()).map_err(|_| fmt::Error)
    }
}

impl<const N: usize> embedded_io::ErrorType for Buffer<N> {
    type Error = SliceWriteError;
}

impl<const N: usize> embedded_io::Write for Buffer<N> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }

        let writable = cmp::min(self.available(), buf.len());
        if writable == 0 {
            Err(SliceWriteError::Full)
        } else {
            self.bytes[self.cursor..self.cursor + writable].copy_from_slice(buf);
            self.cursor += writable;
            Ok(writable)
        }
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<const N: usize> embedded_io_async::Write for Buffer<N> {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        <Self as embedded_io::Write>::write(self, buf)
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        <Self as embedded_io::Write>::flush(self)
    }
}
