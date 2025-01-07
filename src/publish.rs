use core::{fmt::Display, future::Future, ops::Deref};

use embedded_io::Write;
use mqttrs::QoS;

use crate::{io::publish, Error, Payload, Topic, TopicString};

/// A message that can be published to an MQTT broker.
pub trait Publishable {
    /// Write this message's topic into the supplied buffer.
    fn write_topic(&self, buffer: &mut TopicString) -> Result<(), Error>;

    /// Write this message's payload into the supplied buffer.
    fn write_payload(&self, buffer: &mut Payload) -> Result<(), Error>;

    /// Get this message's QoS level.
    fn qos(&self) -> QoS {
        QoS::AtMostOnce
    }

    /// Whether the broker should retain this message.
    fn retain(&self) -> bool {
        false
    }

    /// Publishes this message to the broker. If the stack has not yet been
    /// initialized this is likely to panic.
    fn publish(&self) -> impl Future<Output = Result<(), Error>> {
        async {
            let mut topic = TopicString::new();
            self.write_topic(&mut topic)?;

            let mut payload = Payload::new();
            self.write_payload(&mut payload)?;

            publish(&topic, &payload, self.qos(), self.retain()).await
        }
    }
}

/// A [`Publishable`] with a raw byte payload.
pub struct PublishBytes<'a, T, B: AsRef<[u8]>> {
    pub(crate) topic: &'a Topic<T>,
    pub(crate) data: B,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
}

impl<T, B: AsRef<[u8]>> PublishBytes<'_, T, B> {
    /// Sets the QoS level for this message.
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets whether the broker should retain this message.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl<'a, T: Deref<Target = str> + 'a, B: AsRef<[u8]>> Publishable for PublishBytes<'a, T, B> {
    fn write_topic(&self, buffer: &mut TopicString) -> Result<(), Error> {
        self.topic.to_string(buffer)
    }

    fn write_payload(&self, buffer: &mut Payload) -> Result<(), Error> {
        buffer
            .write_all(self.data.as_ref())
            .map_err(|_| Error::TooLarge)
    }

    fn qos(&self) -> QoS {
        self.qos
    }

    fn retain(&self) -> bool {
        self.retain
    }

    async fn publish(&self) -> Result<(), Error> {
        let mut topic = TopicString::new();
        self.write_topic(&mut topic)?;

        publish(&topic, self.data.as_ref(), self.qos(), self.retain()).await
    }
}

/// A [`Publishable`] with a payload that implements [`Display`].
pub struct PublishDisplay<'a, T, D: Display> {
    pub(crate) topic: &'a Topic<T>,
    pub(crate) data: D,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
}

impl<T, D: Display> PublishDisplay<'_, T, D> {
    /// Sets the QoS level for this message.
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets whether the broker should retain this message.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

impl<'a, T: Deref<Target = str> + 'a, D: Display> Publishable for PublishDisplay<'a, T, D> {
    fn write_topic(&self, buffer: &mut TopicString) -> Result<(), Error> {
        self.topic.to_string(buffer)
    }

    fn write_payload(&self, buffer: &mut Payload) -> Result<(), Error> {
        write!(buffer, "{}", self.data).map_err(|_| Error::TooLarge)
    }

    fn qos(&self) -> QoS {
        self.qos
    }

    fn retain(&self) -> bool {
        self.retain
    }
}

#[cfg(feature = "serde")]
/// A [`Publishable`] with that serializes a JSON payload.
pub struct PublishJson<'a, T, D: serde::Serialize> {
    pub(crate) topic: &'a Topic<T>,
    pub(crate) data: D,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
}

#[cfg(feature = "serde")]
impl <'a, T, D: serde::Serialize> PublishJson<'a, T, D> {

    /// Create new `PublishJson` with given topic, data, QoS and retain
    pub fn new(topic: &'a Topic<T>, data: D, qos: QoS, retain: bool) -> Self {
        Self {
            topic, data, qos, retain
        }
    }

}

#[cfg(feature = "serde")]
impl<T, D: serde::Serialize> PublishJson<'_, T, D> {
    /// Sets the QoS level for this message.
    pub fn qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets whether the broker should retain this message.
    pub fn retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }
}

#[cfg(feature = "serde")]
impl<'a, T: Deref<Target = str> + 'a, D: serde::Serialize> Publishable for PublishJson<'a, T, D> {
    fn write_topic(&self, buffer: &mut TopicString) -> Result<(), Error> {
        self.topic.to_string(buffer)
    }

    fn write_payload(&self, buffer: &mut Payload) -> Result<(), Error> {
        buffer
            .serialize_json(&self.data)
            .map_err(|_| Error::TooLarge)
    }

    fn qos(&self) -> QoS {
        self.qos
    }

    fn retain(&self) -> bool {
        self.retain
    }
}
