use core::{fmt::Display, ops::Deref};

use embassy_futures::select::{select, Either};
use embassy_sync::pubsub::WaitResult;
use embassy_time::Timer;
use heapless::{String, Vec};
use mqttrs::{Packet, QoS, Subscribe, SubscribeReturnCodes, SubscribeTopic, Unsubscribe};

#[cfg(feature = "serde")]
use crate::publish::PublishJson;
use crate::{
    device_id, device_type,
    io::{assign_pid, send_packet, subscribe},
    publish::{PublishBytes, PublishDisplay},
    ControlMessage, Error, TopicString, CONFIRMATION_TIMEOUT,
};

/// An MQTT topic that is optionally prefixed with the device type and unique ID.
/// Normally you will define all your application's topics as consts with static
/// lifetimes.
///
/// A [`Topic`] is the main entry to publishing messages to the broker.
///
/// ```
/// # use mcutie::{Publishable, Topic};
/// const DEVICE_AVAILABILITY: Topic<&'static str> = Topic::Device("state");
///
/// async fn send_status(status: &'static str) {
///   let _ = DEVICE_AVAILABILITY.with_bytes(status.as_bytes()).publish().await;
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub enum Topic<T> {
    /// A topic that is prefixed with the device type.
    DeviceType(T),
    /// A topic that is prefixed with the device type and unique ID.
    Device(T),
    /// Any topic.
    General(T),
}

impl<A, B> PartialEq<Topic<A>> for Topic<B>
where
    B: PartialEq<A>,
{
    fn eq(&self, other: &Topic<A>) -> bool {
        match (self, other) {
            (Topic::DeviceType(l0), Topic::DeviceType(r0)) => l0 == r0,
            (Topic::Device(l0), Topic::Device(r0)) => l0 == r0,
            (Topic::General(l0), Topic::General(r0)) => l0 == r0,
            _ => false,
        }
    }
}

impl<T> Topic<T> {
    /// Creates a publishable message with something that can return a reference
    /// to the payload in bytes.
    ///
    /// Defaults to non-retained with QoS of 0 (AtMostOnce).
    pub fn with_bytes<B: AsRef<[u8]>>(&self, data: B) -> PublishBytes<'_, T, B> {
        PublishBytes {
            topic: self,
            data,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    /// Creates a publishable message with something that implements [`Display`].
    ///
    /// Defaults to non-retained with QoS of 0 (AtMostOnce).
    pub fn with_display<D: Display>(&self, data: D) -> PublishDisplay<'_, T, D> {
        PublishDisplay {
            topic: self,
            data,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    #[cfg(feature = "serde")]
    /// Creates a publishable message with something that can be serialized to
    /// JSON.
    ///
    /// Defaults to non-retained with QoS of 0 (AtMostOnce).
    pub fn with_json<D: serde::Serialize>(&self, data: D) -> PublishJson<'_, T, D> {
        PublishJson {
            topic: self,
            data,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }
}

impl Topic<TopicString> {
    pub(crate) fn from_str(mut st: &str) -> Result<Self, ()> {
        let mut strip_prefix = |pr: &str| -> bool {
            if st.starts_with(pr) && st[pr.len()..].starts_with('/') {
                st = &st[pr.len() + 1..];
                true
            } else {
                false
            }
        };

        if strip_prefix(device_type()) {
            if strip_prefix(device_id()) {
                let mut topic = TopicString::new();
                topic.push_str(st)?;
                Ok(Topic::Device(topic))
            } else {
                let mut topic = TopicString::new();
                topic.push_str(st)?;
                Ok(Topic::DeviceType(topic))
            }
        } else {
            let mut topic = TopicString::new();
            topic.push_str(st)?;
            Ok(Topic::General(topic))
        }
    }
}

impl<T: Deref<Target = str>> Topic<T> {
    pub(crate) fn to_string<const N: usize>(&self, result: &mut String<N>) -> Result<(), Error> {
        match self {
            Topic::Device(st) => {
                result
                    .push_str(device_type())
                    .map_err(|_| Error::TooLarge)?;
                result.push_str("/").map_err(|_| Error::TooLarge)?;
                result.push_str(device_id()).map_err(|_| Error::TooLarge)?;
                result.push_str("/").map_err(|_| Error::TooLarge)?;
                result.push_str(st.as_ref()).map_err(|_| Error::TooLarge)?;
            }
            Topic::DeviceType(st) => {
                result
                    .push_str(device_type())
                    .map_err(|_| Error::TooLarge)?;
                result.push_str("/").map_err(|_| Error::TooLarge)?;
                result.push_str(st.as_ref()).map_err(|_| Error::TooLarge)?;
            }
            Topic::General(st) => {
                result.push_str(st.as_ref()).map_err(|_| Error::TooLarge)?;
            }
        }

        Ok(())
    }

    /// Converts to a topic containing an [`str`]. Particularly useful for converting from an owned
    /// string for match patterns.
    pub fn as_ref(&self) -> Topic<&str> {
        match self {
            Topic::DeviceType(st) => Topic::DeviceType(st.as_ref()),
            Topic::Device(st) => Topic::Device(st.as_ref()),
            Topic::General(st) => Topic::General(st.as_ref()),
        }
    }

    /// Subscribes to this topic. If `wait_for_ack` is true then this will wait until confirmation
    /// is received from the broker before returning.
    pub async fn subscribe(&self, wait_for_ack: bool) -> Result<(), Error> {
        let mut subscriber = subscribe().await;

        let mut topic_path = TopicString::new();
        if self.to_string(&mut topic_path).is_err() {
            return Err(Error::TooLarge);
        }

        let pid = assign_pid().await;

        let subscribe_topic = SubscribeTopic {
            topic_path,
            qos: QoS::AtLeastOnce,
        };

        // The size of this vec must match that used by mqttrs.
        let topics = match Vec::<SubscribeTopic, 5>::from_slice(&[subscribe_topic]) {
            Ok(t) => t,
            Err(_) => return Err(Error::TooLarge),
        };

        let packet = Packet::Subscribe(Subscribe { pid, topics });

        send_packet(packet).await?;

        if wait_for_ack {
            match select(
                async {
                    loop {
                        match subscriber.next_message().await {
                            WaitResult::Lagged(_) => {
                                // Maybe we missed the message?
                            }
                            WaitResult::Message(ControlMessage::Subscribed(
                                subscribed_pid,
                                return_code,
                            )) if subscribed_pid == pid => {
                                if matches!(return_code, SubscribeReturnCodes::Success(_)) {
                                    return Ok(());
                                } else {
                                    return Err(Error::IOError);
                                }
                            }
                            _ => {}
                        }
                    }
                },
                Timer::after_millis(CONFIRMATION_TIMEOUT),
            )
            .await
            {
                Either::First(r) => r,
                Either::Second(_) => Err(Error::TimedOut),
            }
        } else {
            Ok(())
        }
    }

    /// Unsubscribes from a topic. If `wait_for_ack` is true then this will wait until confirmation is
    /// received from the broker before returning.
    pub async fn unsubscribe(&self, wait_for_ack: bool) -> Result<(), Error> {
        let mut subscriber = subscribe().await;

        let mut topic_path = TopicString::new();
        if self.to_string(&mut topic_path).is_err() {
            return Err(Error::TooLarge);
        }

        let pid = assign_pid().await;

        // The size of this vec must match that used by mqttrs.
        let topics = match Vec::<TopicString, 5>::from_slice(&[topic_path]) {
            Ok(t) => t,
            Err(_) => return Err(Error::TooLarge),
        };

        let packet = Packet::Unsubscribe(Unsubscribe { pid, topics });

        send_packet(packet).await?;

        if wait_for_ack {
            match select(
                async {
                    loop {
                        match subscriber.next_message().await {
                            WaitResult::Lagged(_) => {
                                // Maybe we missed the message?
                            }
                            WaitResult::Message(ControlMessage::Unsubscribed(subscribed_pid))
                                if subscribed_pid == pid =>
                            {
                                return Ok(());
                            }
                            _ => {}
                        }
                    }
                },
                Timer::after_millis(CONFIRMATION_TIMEOUT),
            )
            .await
            {
                Either::First(r) => r,
                Either::Second(_) => Err(Error::TimedOut),
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use heapless::String;

    use super::Topic;
    use crate::{test_support::init_device, Error, TopicString};

    fn path<T: core::ops::Deref<Target = str>>(topic: &Topic<T>) -> TopicString {
        let mut result = TopicString::new();
        topic.to_string(&mut result).unwrap();
        result
    }

    #[test]
    fn equality() {
        assert_eq!(Topic::Device("a"), Topic::Device("a"));
        assert_eq!(Topic::DeviceType("a"), Topic::DeviceType("a"));
        assert_eq!(Topic::General("a"), Topic::General("a"));
        assert_ne!(Topic::Device("a"), Topic::Device("b"));
        assert_ne!(Topic::Device("a"), Topic::DeviceType("a"));
        assert_ne!(Topic::Device("a"), Topic::General("a"));
        assert_ne!(Topic::DeviceType("a"), Topic::General("a"));

        let owned = Topic::General(TopicString::from("a"));
        assert_eq!(owned, Topic::General("a"));
    }

    #[test]
    fn as_ref() {
        let owned = TopicString::from("x/y");
        assert_eq!(Topic::Device(owned.clone()).as_ref(), Topic::Device("x/y"));
        assert_eq!(
            Topic::DeviceType(owned.clone()).as_ref(),
            Topic::DeviceType("x/y")
        );
        assert_eq!(Topic::General(owned).as_ref(), Topic::General("x/y"));
    }

    #[test]
    fn to_string() {
        init_device();
        assert_eq!(path(&Topic::Device("state")), "testdev/0123456789ab/state");
        assert_eq!(path(&Topic::DeviceType("state")), "testdev/state");
        assert_eq!(path(&Topic::General("some/topic")), "some/topic");
    }

    #[test]
    fn to_string_too_large() {
        init_device();
        for topic in [
            Topic::Device("abcdef"),
            Topic::DeviceType("abcdef"),
            Topic::General("abcdef"),
        ] {
            let mut small = String::<5>::new();
            assert!(matches!(topic.to_string(&mut small), Err(Error::TooLarge)));
        }

        // Fails part way through the prefix.
        let mut small = String::<10>::new();
        assert!(matches!(
            Topic::Device("x").to_string(&mut small),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn from_str() {
        init_device();
        assert_eq!(
            Topic::from_str("testdev/0123456789ab/state").unwrap(),
            Topic::Device("state")
        );
        assert_eq!(
            Topic::from_str("testdev/state").unwrap(),
            Topic::DeviceType("state")
        );
        assert_eq!(
            Topic::from_str("testdev/other/state").unwrap(),
            Topic::DeviceType("other/state")
        );
        assert_eq!(
            Topic::from_str("other/state").unwrap(),
            Topic::General("other/state")
        );
        // A prefix match must be followed by a separator.
        assert_eq!(
            Topic::from_str("testdevice/state").unwrap(),
            Topic::General("testdevice/state")
        );
        assert_eq!(
            Topic::from_str("testdev/0123456789abc/x").unwrap(),
            Topic::DeviceType("0123456789abc/x")
        );
    }

    #[test]
    fn from_str_exact_prefix() {
        init_device();
        assert_eq!(
            Topic::from_str("testdev").unwrap(),
            Topic::General("testdev")
        );
        assert_eq!(Topic::from_str("testdev/").unwrap(), Topic::DeviceType(""));
        assert_eq!(
            Topic::from_str("testdev/0123456789ab").unwrap(),
            Topic::DeviceType("0123456789ab")
        );
    }

    #[test]
    fn round_trip() {
        init_device();
        for topic in [
            Topic::Device("a/b"),
            Topic::DeviceType("c"),
            Topic::General("d/e/f"),
        ] {
            assert_eq!(Topic::from_str(&path(&topic)).unwrap(), topic);
        }
    }
}
