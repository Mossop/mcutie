#![no_std]
#![doc = include_str!("../README.md")]
#![deny(unreachable_pub)]
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

use core::{ops::Deref, str};

pub use buffer::Buffer;
use embassy_net::{HardwareAddress, Stack};
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, channel::Channel};
use heapless::String;
pub use io::McutieTask;
pub use mqttrs::QoS;
use mqttrs::{Pid, SubscribeReturnCodes};
use once_cell::sync::OnceCell;
pub use publish::*;
pub use topic::Topic;

// This must come first so the macros are visible
pub(crate) mod fmt;

mod buffer;
#[cfg(feature = "homeassistant")]
pub mod homeassistant;
mod io;
mod pipe;
mod publish;
mod topic;

// This really needs to match that used by mqttrs.
const TOPIC_LENGTH: usize = 256;
const PAYLOAD_LENGTH: usize = 2048;

/// A fixed length stack allocated string. The length is fixed by the mqttrs crate.
pub type TopicString = String<TOPIC_LENGTH>;
/// A fixed length buffer of 2048 bytes.
pub type Payload = Buffer<PAYLOAD_LENGTH>;

// By default in the event of an error connecting to the broker we will wait for 5s.
const DEFAULT_BACKOFF: u64 = 5000;
// If the connection dropped then re-connect more quickly.
const RESET_BACKOFF: u64 = 200;
// How long to wait for the broker to confirm actions.
const CONFIRMATION_TIMEOUT: u64 = 2000;

static DATA_CHANNEL: Channel<CriticalSectionRawMutex, MqttMessage, 10> = Channel::new();

static DEVICE_TYPE: OnceCell<String<32>> = OnceCell::new();
static DEVICE_ID: OnceCell<String<32>> = OnceCell::new();

fn device_id() -> &'static str {
    DEVICE_ID.get().unwrap()
}

fn device_type() -> &'static str {
    DEVICE_TYPE.get().unwrap()
}

/// Various errors
#[derive(Debug)]
pub enum Error {
    /// An IO error occured.
    IOError,
    /// The operation timed out.
    TimedOut,
    /// An attempt was made to encode something too large.
    TooLarge,
    /// A packet or payload could not be decoded or encoded.
    PacketError,
    /// An invalid or unsupported operation was attempted.
    Invalid,
}

#[allow(clippy::large_enum_variant)]
/// A message from the MQTT broker.
pub enum MqttMessage {
    /// The broker has been connected to successfully. Generally in response to this message a
    /// device should subscribe to topics of interest and send out any device state.
    Connected,
    /// New data received from the broker.
    Publish(Topic<TopicString>, Payload),
    /// The connection to the broker has been dropped.
    Disconnected,
    /// Home Assistant has come online and you should send any discovery messages.
    #[cfg(feature = "homeassistant")]
    HomeAssistantOnline,
}

#[derive(Clone)]
enum ControlMessage {
    Published(Pid),
    Subscribed(Pid, SubscribeReturnCodes),
    Unsubscribed(Pid),
}

/// Receives messages from the broker.
pub struct McutieReceiver;

impl McutieReceiver {
    /// Waits for the next message from the broker.
    pub async fn receive(&self) -> MqttMessage {
        DATA_CHANNEL.receive().await
    }
}

/// A builder to configure the MQTT stack.
pub struct McutieBuilder<'t, T, L, const S: usize>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    network: Stack<'t>,
    device_type: &'t str,
    device_id: Option<&'t str>,
    broker: &'t str,
    last_will: Option<L>,
    username: Option<&'t str>,
    password: Option<&'t str>,
    subscriptions: [Topic<T>; S],
}

impl<'t, T: Deref<Target = str> + 't, L: Publishable + 't> McutieBuilder<'t, T, L, 0> {
    /// Creates a new builder with the initial required configuration.
    ///
    /// `device_type` is expected to be the same for all devices of the same type.
    /// `broker` may be an IP address or a DNS name for the broker to connect to.
    pub fn new(network: Stack<'t>, device_type: &'t str, broker: &'t str) -> Self {
        Self {
            network,
            device_type,
            broker,
            device_id: None,
            last_will: None,
            username: None,
            password: None,
            subscriptions: [],
        }
    }
}

impl<'t, T: Deref<Target = str> + 't, L: Publishable + 't, const S: usize>
    McutieBuilder<'t, T, L, S>
{
    /// Add some default topics to subscribe to.
    pub fn with_subscriptions<const N: usize>(
        self,
        subscriptions: [Topic<T>; N],
    ) -> McutieBuilder<'t, T, L, N> {
        McutieBuilder {
            network: self.network,
            device_type: self.device_type,
            broker: self.broker,
            device_id: self.device_id,
            last_will: self.last_will,
            username: self.username,
            password: self.password,
            subscriptions,
        }
    }
}

impl<'t, T: Deref<Target = str> + 't, L: Publishable + 't, const S: usize>
    McutieBuilder<'t, T, L, S>
{
    /// Adds authentication for the broker.
    pub fn with_authentication(self, username: &'t str, password: &'t str) -> Self {
        Self {
            username: Some(username),
            password: Some(password),
            ..self
        }
    }

    /// Sets a last will message to be published in the event of disconnection.
    pub fn with_last_will(self, last_will: L) -> Self {
        Self {
            last_will: Some(last_will),
            ..self
        }
    }

    /// Sets a custom unique device identifier. If none is set then the network
    /// MAC address is used.
    pub fn with_device_id(self, device_id: &'t str) -> Self {
        Self {
            device_id: Some(device_id),
            ..self
        }
    }

    /// Initialises the MQTT stack returning a receiver for listening to
    /// messages from the broker and a future that must be run in order for the
    /// stack to operate.
    pub fn build(self) -> (McutieReceiver, McutieTask<'t, T, L, S>) {
        let mut dtype = String::<32>::new();
        dtype.push_str(self.device_type).unwrap();
        DEVICE_TYPE.set(dtype).unwrap();

        let mut did = String::<32>::new();
        if let Some(device_id) = self.device_id {
            did.push_str(device_id).unwrap();
        } else if let HardwareAddress::Ethernet(address) = self.network.hardware_address() {
            let mut buffer = [0_u8; 12];
            hex::encode_to_slice(address.as_bytes(), &mut buffer).unwrap();
            did.push_str(str::from_utf8(&buffer).unwrap()).unwrap();
        }

        DEVICE_ID.set(did).unwrap();

        (
            McutieReceiver {},
            McutieTask {
                network: self.network,
                broker: self.broker,
                last_will: self.last_will,
                username: self.username,
                password: self.password,
                subscriptions: self.subscriptions,
            },
        )
    }
}
