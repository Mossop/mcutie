//! Shared helpers for tests that run on the host.
use core::{cell::RefCell, future::Future, ops::Deref};
use std::{
    sync::{Mutex, MutexGuard},
    vec::Vec,
};

use embassy_futures::select::{select, Either};
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, pipe::Pipe};
use embassy_time::{with_timeout, Duration};
use heapless::String;
use mqttrs::{decode_slice, encode_slice, Connack, ConnectReturnCode, Packet, QoS};

use crate::{
    io::{packet_size, Connection},
    MqttMessage, Publishable, DATA_CHANNEL, DEVICE_ID, DEVICE_TYPE,
};

pub(crate) const TEST_DEVICE_TYPE: &str = "testdev";
pub(crate) const TEST_DEVICE_ID: &str = "0123456789ab";

/// Serializes tests that rely on the crate's global channels.
static GLOBAL_LOCK: Mutex<()> = Mutex::new(());

/// Initialises the global device type and ID. Safe to call from any test.
pub(crate) fn init_device() {
    DEVICE_TYPE.get_or_init(|| {
        let mut s = String::new();
        s.push_str(TEST_DEVICE_TYPE).unwrap();
        s
    });
    DEVICE_ID.get_or_init(|| {
        let mut s = String::new();
        s.push_str(TEST_DEVICE_ID).unwrap();
        s
    });
}

/// Takes exclusive access to the global channels and clears out anything left
/// over from a previous test.
pub(crate) fn lock_globals() -> MutexGuard<'static, ()> {
    let guard = GLOBAL_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    init_device();
    while DATA_CHANNEL.try_receive().is_ok() {}
    guard
}

/// Runs a future to completion, failing if it takes too long.
pub(crate) fn run<F: Future>(future: F) -> F::Output {
    futures_executor::block_on(with_timeout(Duration::from_secs(5), future))
        .expect("test timed out")
}

/// Encodes a packet into a byte vector.
pub(crate) fn encode(packet: &Packet<'_>) -> Vec<u8> {
    let mut buffer = [0_u8; 4096];
    let len = encode_slice(packet, &mut buffer).unwrap();
    buffer[..len].to_vec()
}

/// Decodes a single complete packet.
pub(crate) fn decode(bytes: &[u8]) -> Packet<'_> {
    decode_slice(bytes).unwrap().expect("incomplete packet")
}

/// An in-memory transport with a scripted broker at the other end.
pub(crate) struct FakeBroker {
    to_client: Pipe<CriticalSectionRawMutex, 4096>,
    to_broker: Pipe<CriticalSectionRawMutex, 4096>,
    pending: RefCell<Vec<u8>>,
}

impl FakeBroker {
    pub(crate) fn new() -> Self {
        Self {
            to_client: Pipe::new(),
            to_broker: Pipe::new(),
            pending: RefCell::new(Vec::new()),
        }
    }

    /// The transport halves to hand to the client.
    pub(crate) fn client(
        &self,
    ) -> (
        &Pipe<CriticalSectionRawMutex, 4096>,
        &Pipe<CriticalSectionRawMutex, 4096>,
    ) {
        (&self.to_client, &self.to_broker)
    }

    /// Sends raw bytes to the client.
    pub(crate) async fn send_raw(&self, bytes: &[u8]) {
        let mut written = 0;
        while written < bytes.len() {
            written += self.to_client.write(&bytes[written..]).await;
        }
    }

    /// Sends a packet to the client.
    pub(crate) async fn send(&self, packet: &Packet<'_>) {
        self.send_raw(&encode(packet)).await;
    }

    /// Waits for the next complete packet from the client and returns its raw
    /// bytes. Use [`decode`] to inspect it.
    pub(crate) async fn receive(&self) -> Vec<u8> {
        loop {
            {
                let mut pending = self.pending.borrow_mut();
                if let Some(len) = packet_size(&pending) {
                    assert_ne!(len, 0, "client sent an invalid packet");
                    if len <= pending.len() {
                        return pending.drain(..len).collect();
                    }
                }
            }

            let mut buffer = [0_u8; 1024];
            let len = self.to_broker.read(&mut buffer).await;
            self.pending.borrow_mut().extend_from_slice(&buffer[..len]);
        }
    }
}

/// Serializes a value to a JSON string using the same serializer as the crate.
#[cfg(feature = "homeassistant")]
pub(crate) fn to_json<T: serde::Serialize>(value: &T) -> std::string::String {
    let mut payload = crate::Payload::new();
    payload.serialize_json(value).unwrap();
    std::string::String::from_utf8(payload.to_vec()).unwrap()
}

/// Runs `script` while serving `connection` over the broker's transport.
/// Panics if the connection ends before the script completes.
pub(crate) async fn drive<T, L, const S: usize, F>(
    connection: &Connection<'_, T, L, S>,
    broker: &FakeBroker,
    script: F,
) -> F::Output
where
    T: Deref<Target = str>,
    L: Publishable,
    F: Future,
{
    let (reader, writer) = broker.client();
    match select(connection.serve(reader, writer), script).await {
        Either::First(()) => panic!("connection ended unexpectedly"),
        Either::Second(result) => result,
    }
}

/// Performs the connection handshake from the broker side, accepting the
/// connection and receiving the client's `count` initial subscriptions, which
/// are not acknowledged. Returns the subscribed topics.
pub(crate) async fn handshake(broker: &FakeBroker, count: usize) -> Vec<std::string::String> {
    let connect = broker.receive().await;
    assert!(matches!(decode(&connect), Packet::Connect(_)));

    broker
        .send(&Packet::Connack(Connack {
            session_present: false,
            code: ConnectReturnCode::Accepted,
        }))
        .await;

    let mut topics = Vec::new();
    for _ in 0..count {
        match decode(&broker.receive().await) {
            Packet::Subscribe(subscribe) => {
                for topic in subscribe.topics {
                    assert_eq!(topic.qos, QoS::AtLeastOnce);
                    topics.push(topic.topic_path.as_str().into());
                }
            }
            p => panic!("unexpected packet {:?}", p.get_type()),
        }
    }

    assert!(matches!(
        DATA_CHANNEL.receive().await,
        MqttMessage::Connected
    ));
    topics
}

/// The number of subscriptions the client always makes on connection.
pub(crate) const BUILTIN_SUBSCRIPTIONS: usize = if cfg!(feature = "homeassistant") {
    1
} else {
    0
};
