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
            self.bytes[self.cursor..self.cursor + writable].copy_from_slice(&buf[..writable]);
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

#[cfg(test)]
mod tests {
    use embedded_io::{SliceWriteError, Write};
    use mqttrs::{decode_slice, Packet, Pid};

    use super::Buffer;
    use crate::Error;

    #[test]
    fn new_buffer_is_empty() {
        let buffer = Buffer::<8>::new();
        assert_eq!(&*buffer, b"");
        assert_eq!(buffer.available(), 8);
    }

    #[test]
    fn from_slice() {
        let buffer = Buffer::<8>::from(b"hello").unwrap();
        assert_eq!(&*buffer, b"hello");
        assert_eq!(buffer.available(), 3);

        let buffer = Buffer::<5>::from(b"hello").unwrap();
        assert_eq!(buffer.available(), 0);

        assert!(matches!(Buffer::<4>::from(b"hello"), Err(Error::TooLarge)));
    }

    #[test]
    fn write_until_full() {
        let mut buffer = Buffer::<4>::new();
        assert_eq!(buffer.write(b"").unwrap(), 0);
        assert_eq!(buffer.write(b"ab").unwrap(), 2);
        // Partial writes return how much was written.
        assert_eq!(buffer.write(b"cdef").unwrap(), 2);
        assert_eq!(&*buffer, b"abcd");
        assert_eq!(buffer.write(b"g"), Err(SliceWriteError::Full));
        // Empty writes always succeed.
        assert_eq!(buffer.write(b"").unwrap(), 0);
        buffer.flush().unwrap();
    }

    #[test]
    fn fmt_write() {
        let mut buffer = Buffer::<8>::new();
        core::fmt::Write::write_fmt(&mut buffer, format_args!("{}-{}", 12, "ab")).unwrap();
        assert_eq!(&*buffer, b"12-ab");

        assert!(core::fmt::Write::write_str(&mut buffer, "toolong").is_err());
    }

    #[test]
    fn async_write() {
        let mut buffer = Buffer::<8>::new();
        futures_executor::block_on(async {
            embedded_io_async::Write::write_all(&mut buffer, b"abc")
                .await
                .unwrap();
            embedded_io_async::Write::flush(&mut buffer).await.unwrap();
        });
        assert_eq!(&*buffer, b"abc");
    }

    #[test]
    fn encode_packet() {
        let mut buffer = Buffer::<64>::new();
        buffer.encode_packet(&Packet::Pingreq).unwrap();
        buffer
            .encode_packet(&Packet::Puback(Pid::try_from(5).unwrap()))
            .unwrap();

        assert_eq!(&*buffer, &[0xc0, 0x00, 0x40, 0x02, 0x00, 0x05]);
        assert_eq!(decode_slice(&buffer).unwrap(), Some(Packet::Pingreq));

        let mut small = Buffer::<1>::new();
        assert!(small.encode_packet(&Packet::Pingreq).is_err());
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json_round_trip() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Data<'a> {
            name: &'a str,
            value: u32,
        }

        let mut buffer = Buffer::<64>::new();
        buffer
            .serialize_json(&Data {
                name: "foo",
                value: 42,
            })
            .unwrap();
        assert_eq!(&*buffer, br#"{"name":"foo","value":42}"#);

        let data: Data<'_> = buffer.deserialize_json().unwrap();
        assert_eq!(
            data,
            Data {
                name: "foo",
                value: 42
            }
        );

        let mut small = Buffer::<4>::new();
        assert!(small.serialize_json(&data).is_err());

        let invalid = Buffer::<8>::from(b"{").unwrap();
        assert!(invalid.deserialize_json::<Data<'_>>().is_err());
    }
}
