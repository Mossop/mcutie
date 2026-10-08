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

#[cfg(test)]
mod tests {
    use mqttrs::QoS;

    use super::Publishable;
    use crate::{test_support::init_device, Error, Payload, Topic, TopicString, PAYLOAD_LENGTH};

    fn topic_of(p: &impl Publishable) -> TopicString {
        let mut topic = TopicString::new();
        p.write_topic(&mut topic).unwrap();
        topic
    }

    fn payload_of(p: &impl Publishable) -> Payload {
        let mut payload = Payload::new();
        p.write_payload(&mut payload).unwrap();
        payload
    }

    #[test]
    fn bytes() {
        init_device();
        let topic = Topic::Device("state");

        let message = topic.with_bytes(b"on");
        assert_eq!(Publishable::qos(&message), QoS::AtMostOnce);
        assert!(!Publishable::retain(&message));
        assert_eq!(topic_of(&message), "testdev/0123456789ab/state");
        assert_eq!(&*payload_of(&message), b"on");

        let message = topic.with_bytes("off").qos(QoS::AtLeastOnce).retain(true);
        assert_eq!(Publishable::qos(&message), QoS::AtLeastOnce);
        assert!(Publishable::retain(&message));
        assert_eq!(&*payload_of(&message), b"off");
    }

    #[test]
    fn bytes_too_large() {
        let topic = Topic::General("big");
        let data = [0_u8; PAYLOAD_LENGTH + 1];
        let mut payload = Payload::new();
        assert!(matches!(
            topic.with_bytes(data).write_payload(&mut payload),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn display() {
        init_device();
        let topic = Topic::DeviceType("temp");

        let message = topic.with_display(21.5);
        assert_eq!(Publishable::qos(&message), QoS::AtMostOnce);
        assert!(!Publishable::retain(&message));
        assert_eq!(topic_of(&message), "testdev/temp");
        assert_eq!(&*payload_of(&message), b"21.5");

        let message = topic
            .with_display("hello")
            .qos(QoS::ExactlyOnce)
            .retain(true);
        assert_eq!(Publishable::qos(&message), QoS::ExactlyOnce);
        assert!(Publishable::retain(&message));
    }

    #[test]
    fn display_too_large() {
        struct Huge;
        impl core::fmt::Display for Huge {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                for _ in 0..=PAYLOAD_LENGTH {
                    f.write_str("x")?;
                }
                Ok(())
            }
        }

        let mut payload = Payload::new();
        assert!(matches!(
            Topic::General("t")
                .with_display(Huge)
                .write_payload(&mut payload),
            Err(Error::TooLarge)
        ));
    }

    #[cfg(feature = "serde")]
    #[test]
    fn json() {
        #[derive(serde::Serialize)]
        struct Data {
            value: u8,
        }

        let topic = Topic::General("data");
        let message = topic.with_json(Data { value: 3 });
        assert_eq!(Publishable::qos(&message), QoS::AtMostOnce);
        assert!(!Publishable::retain(&message));
        assert_eq!(topic_of(&message), "data");
        assert_eq!(&*payload_of(&message), br#"{"value":3}"#);

        let message = topic
            .with_json(Data { value: 3 })
            .qos(QoS::AtLeastOnce)
            .retain(true);
        assert_eq!(Publishable::qos(&message), QoS::AtLeastOnce);
        assert!(Publishable::retain(&message));
    }

    #[test]
    fn topic_too_large() {
        let long = [b'a'; 300];
        let topic = Topic::General(core::str::from_utf8(&long).unwrap());
        let mut buffer = TopicString::new();
        assert!(matches!(
            topic.with_bytes(b"").write_topic(&mut buffer),
            Err(Error::TooLarge)
        ));
    }
}
