use core::ops::Deref;

pub(crate) use atomic16::assign_pid;
use embassy_futures::select::{select, select3, Either};
use embassy_net::{dns::DnsQueryType, tcp::TcpSocket, Stack};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    pubsub::{PubSubChannel, Subscriber, WaitResult},
};
use embassy_time::Timer;
use embedded_io_async::{Read, Write};
use mqttrs::{
    decode_slice, Connect, ConnectReturnCode, LastWill, Packet, Pid, Protocol, Publish, QoS, QosPid,
};

use crate::{
    device_id, fmt::Debug2Format, pipe::ConnectedPipe, ControlMessage, Error, MqttMessage, Payload,
    Publishable, Topic, TopicString, CONFIRMATION_TIMEOUT, DATA_CHANNEL, DEFAULT_BACKOFF,
    RESET_BACKOFF,
};

static SEND_QUEUE: ConnectedPipe<CriticalSectionRawMutex, Payload, 10> = ConnectedPipe::new();

pub(crate) static CONTROL_CHANNEL: PubSubChannel<CriticalSectionRawMutex, ControlMessage, 2, 5, 0> =
    PubSubChannel::new();

type ControlSubscriber = Subscriber<'static, CriticalSectionRawMutex, ControlMessage, 2, 5, 0>;

pub(crate) async fn subscribe() -> ControlSubscriber {
    loop {
        if let Ok(sub) = CONTROL_CHANNEL.subscriber() {
            return sub;
        }

        Timer::after_millis(50).await;
    }
}

#[cfg(target_has_atomic = "16")]
mod atomic16 {
    use core::sync::atomic::{AtomicU16, Ordering};

    use mqttrs::Pid;

    static PID: AtomicU16 = AtomicU16::new(0);

    pub(crate) async fn assign_pid() -> Pid {
        Pid::new() + PID.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(not(target_has_atomic = "16"))]
mod atomic16 {
    use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, mutex::Mutex};
    use mqttrs::Pid;

    static PID_MUTEX: Mutex<CriticalSectionRawMutex, u16> = Mutex::new(0);

    pub(crate) async fn assign_pid() -> Pid {
        let mut locked = PID_MUTEX.lock().await;
        *locked += 1;

        Pid::new() + *locked
    }
}

pub(crate) async fn send_packet(packet: Packet<'_>) -> Result<(), Error> {
    let mut buffer = Payload::new();

    match buffer.encode_packet(&packet) {
        Ok(()) => {
            debug!(
                "Sending packet to broker: {:?}",
                Debug2Format(&packet.get_type())
            );
            SEND_QUEUE.push(buffer).await;
            Ok(())
        }
        Err(_) => {
            error!("Failed to send packet");
            Err(Error::PacketError)
        }
    }
}

pub(crate) async fn wait_for_publish(
    mut subscriber: ControlSubscriber,
    expected_pid: Pid,
) -> Result<(), Error> {
    match select(
        async {
            loop {
                match subscriber.next_message().await {
                    WaitResult::Lagged(_) => {
                        // Maybe we missed the message?
                    }
                    WaitResult::Message(ControlMessage::Published(published_pid))
                        if published_pid == expected_pid =>
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
}

pub(crate) async fn publish(
    topic_name: &str,
    payload: &[u8],
    qos: QoS,
    retain: bool,
) -> Result<(), Error> {
    let subscriber = subscribe().await;

    let (qospid, pid) = match qos {
        QoS::AtMostOnce => (QosPid::AtMostOnce, None),
        QoS::AtLeastOnce => {
            let pid = assign_pid().await;
            (QosPid::AtLeastOnce(pid), Some(pid))
        }
        QoS::ExactlyOnce => {
            let pid = assign_pid().await;
            (QosPid::ExactlyOnce(pid), Some(pid))
        }
    };

    let packet = Packet::Publish(Publish {
        dup: false,
        qospid,
        retain,
        topic_name,
        payload,
    });

    send_packet(packet).await?;

    if let Some(expected_pid) = pid {
        wait_for_publish(subscriber, expected_pid).await
    } else {
        Ok(())
    }
}

pub(crate) fn packet_size(buffer: &[u8]) -> Option<usize> {
    let mut pos = 1;
    let mut multiplier = 1;
    let mut value = 0;

    while pos < buffer.len() {
        value += (buffer[pos] & 127) as usize * multiplier;
        multiplier *= 128;

        if (buffer[pos] & 128) == 0 {
            return Some(value + pos + 1);
        }

        pos += 1;
        if pos == 5 {
            return Some(0);
        }
    }

    None
}

/// The MQTT task that must be run in order for the stack to operate.
pub struct McutieTask<'t, T, L, const S: usize>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    pub(crate) network: Stack<'t>,
    pub(crate) broker: &'t str,
    pub(crate) connection: Connection<'t, T, L, S>,
}

/// The configuration and protocol handling for a single connection to the
/// broker, independent of the underlying transport.
pub(crate) struct Connection<'t, T, L, const S: usize>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    pub(crate) last_will: Option<L>,
    pub(crate) username: Option<&'t str>,
    pub(crate) password: Option<&'t str>,
    pub(crate) subscriptions: [Topic<T>; S],
}

impl<'t, T, L, const S: usize> Connection<'t, T, L, S>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    #[cfg(not(feature = "homeassistant"))]
    async fn ha_handle_update(&self, _topic: &Topic<TopicString>, _payload: &Payload) -> bool {
        false
    }

    async fn recv_loop<R: Read>(&self, mut reader: R) -> Result<(), Error> {
        let mut buffer = [0_u8; 4096];
        let mut cursor: usize = 0;

        let controller = CONTROL_CHANNEL.immediate_publisher();

        loop {
            match reader.read(&mut buffer[cursor..]).await {
                Ok(0) => {
                    error!("Receive socket closed");
                    return Ok(());
                }
                Ok(len) => {
                    cursor += len;
                }
                Err(_) => {
                    error!("I/O failure reading packet");
                    return Err(Error::IOError);
                }
            }

            let mut start_pos = 0;
            loop {
                let packet_length = match packet_size(&buffer[start_pos..cursor]) {
                    Some(0) => {
                        error!("Invalid MQTT packet");
                        return Err(Error::PacketError);
                    }
                    Some(len) if len > buffer.len() => {
                        error!("MQTT packet too large to receive");
                        return Err(Error::PacketError);
                    }
                    Some(len) if start_pos + len <= cursor => len,
                    _ => {
                        // Not enough data has been received yet to decode the next packet.
                        if start_pos != 0 {
                            // Adjust the buffer to reclaim any unused data
                            buffer.copy_within(start_pos..cursor, 0);
                            cursor -= start_pos;
                        }
                        break;
                    }
                };

                let packet = match decode_slice(&buffer[start_pos..(start_pos + packet_length)]) {
                    Ok(Some(p)) => p,
                    Ok(None) => {
                        error!("Packet length calculation failed.");
                        return Err(Error::PacketError);
                    }
                    Err(_) => {
                        error!("Invalid MQTT packet");
                        return Err(Error::PacketError);
                    }
                };

                debug!(
                    "Received packet from broker: {:?}",
                    Debug2Format(&packet.get_type())
                );

                match packet {
                    Packet::Connack(connack) => match connack.code {
                        ConnectReturnCode::Accepted => {
                            #[cfg(feature = "homeassistant")]
                            self.ha_after_connected().await;

                            for topic in &self.subscriptions {
                                let _ = topic.subscribe(false).await;
                            }

                            DATA_CHANNEL.send(MqttMessage::Connected).await;
                        }
                        _ => {
                            error!("Connection request to broker was not accepted");
                            return Err(Error::IOError);
                        }
                    },
                    Packet::Pingresp => {}

                    Packet::Publish(publish) => {
                        match (
                            Topic::from_str(publish.topic_name),
                            Payload::from(publish.payload),
                        ) {
                            (Ok(topic), Ok(payload)) => {
                                if !self.ha_handle_update(&topic, &payload).await {
                                    DATA_CHANNEL
                                        .send(MqttMessage::Publish(topic, payload))
                                        .await;
                                }
                            }
                            _ => {
                                error!("Unable to process publish data as it was too large");
                            }
                        }

                        match publish.qospid {
                            mqttrs::QosPid::AtMostOnce => {}
                            mqttrs::QosPid::AtLeastOnce(pid) => {
                                send_packet(Packet::Puback(pid)).await?;
                            }
                            mqttrs::QosPid::ExactlyOnce(pid) => {
                                send_packet(Packet::Pubrec(pid)).await?;
                            }
                        }
                    }
                    Packet::Puback(pid) => {
                        controller.publish_immediate(ControlMessage::Published(pid));
                    }
                    Packet::Pubrec(pid) => {
                        controller.publish_immediate(ControlMessage::Published(pid));
                        send_packet(Packet::Pubrel(pid)).await?;
                    }
                    Packet::Pubrel(pid) => send_packet(Packet::Pubcomp(pid)).await?,
                    Packet::Pubcomp(_) => {}

                    Packet::Suback(suback) => {
                        if let Some(return_code) = suback.return_codes.first() {
                            controller.publish_immediate(ControlMessage::Subscribed(
                                suback.pid,
                                *return_code,
                            ));
                        } else {
                            warn!("Unexpected suback with no return codes");
                        }
                    }
                    Packet::Unsuback(pid) => {
                        controller.publish_immediate(ControlMessage::Unsubscribed(pid));
                    }

                    Packet::Connect(_)
                    | Packet::Subscribe(_)
                    | Packet::Pingreq
                    | Packet::Unsubscribe(_)
                    | Packet::Disconnect => {
                        debug!(
                            "Unexpected packet from broker: {:?}",
                            Debug2Format(&packet.get_type())
                        );
                    }
                }

                start_pos += packet_length;
                if start_pos == cursor {
                    cursor = 0;
                    break;
                }
            }
        }
    }

    async fn write_loop<W: Write>(&self, mut writer: W) {
        let mut buffer = Payload::new();

        let mut last_will_topic = TopicString::new();
        let mut last_will_payload = Payload::new();

        let last_will = self.last_will.as_ref().and_then(|p| {
            if p.write_topic(&mut last_will_topic).is_ok()
                && p.write_payload(&mut last_will_payload).is_ok()
            {
                Some(LastWill {
                    topic: &last_will_topic,
                    message: &last_will_payload,
                    qos: p.qos(),
                    retain: p.retain(),
                })
            } else {
                None
            }
        });

        // Send our connection request.
        if buffer
            .encode_packet(&Packet::Connect(Connect {
                protocol: Protocol::MQTT311,
                keep_alive: 60,
                client_id: device_id(),
                clean_session: true,
                last_will,
                username: self.username,
                password: self.password.map(|s| s.as_bytes()),
            }))
            .is_err()
        {
            error!("Failed to encode connection packet");
            return;
        }

        if let Err(e) = writer.write_all(&buffer).await {
            error!("Failed to send connection packet: {:?}", Debug2Format(&e));
            return;
        }

        let reader = SEND_QUEUE.reader();

        loop {
            let buffer = reader.receive().await;

            trace!("Writer sending packet");
            if let Err(e) = writer.write_all(&buffer).await {
                error!("Failed to send data: {:?}", Debug2Format(&e));
                return;
            }
        }
    }

    /// Runs the MQTT protocol over an established transport until either side
    /// fails or the connection is closed.
    pub(crate) async fn serve<R: Read, W: Write>(&self, reader: R, writer: W) {
        let ping_loop = async {
            loop {
                Timer::after_secs(45).await;

                let _ = send_packet(Packet::Pingreq).await;
            }
        };

        select3(self.write_loop(writer), ping_loop, self.recv_loop(reader)).await;
    }
}

impl<'t, T, L, const S: usize> McutieTask<'t, T, L, S>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    /// Runs the MQTT stack. The future returned from this must be awaited for everything to work.
    pub async fn run(self) -> ! {
        let mut timeout: Option<u64> = None;

        let mut rx_buffer = [0; 4096];
        let mut tx_buffer = [0; 4096];

        loop {
            if let Some(millis) = timeout.replace(DEFAULT_BACKOFF) {
                Timer::after_millis(millis).await;
            }

            if !self.network.is_config_up() {
                debug!("Waiting for network to configure.");
                self.network.wait_config_up().await;
                debug!("Network configured.");
            }

            let ip_addrs = match self.network.dns_query(self.broker, DnsQueryType::A).await {
                Ok(v) => v,
                Err(e) => {
                    error!("Failed to lookup '{}' for broker: {:?}", self.broker, e);
                    continue;
                }
            };

            let ip = match ip_addrs.first() {
                Some(i) => *i,
                None => {
                    error!("No IP address found for broker '{}'", self.broker);
                    continue;
                }
            };

            debug!("Connecting to {}:1883", ip);

            let mut socket = TcpSocket::new(self.network, &mut rx_buffer, &mut tx_buffer);
            if let Err(e) = socket.connect((ip, 1883)).await {
                error!("Failed to connect to {}:1883: {:?}", ip, e);
                continue;
            }

            info!("Connected to {}", self.broker);
            timeout = Some(RESET_BACKOFF);

            let (reader, writer) = socket.split();

            let link_down = async {
                self.network.wait_link_down().await;
                warn!("Network link lost");
            };

            let ip_down = async {
                self.network.wait_config_down().await;
                warn!("Network config lost");
            };

            select(
                self.connection.serve(reader, writer),
                select(link_down, ip_down),
            )
            .await;

            socket.close();

            warn!("Lost connection with broker");
            DATA_CHANNEL.send(MqttMessage::Disconnected).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use embassy_futures::join::join;
    use embassy_time::Timer;
    use mqttrs::{
        Connack, ConnectReturnCode, Packet, Pid, Protocol, Publish, QoS, QosPid, Suback,
        SubscribeReturnCodes,
    };

    use super::{packet_size, Connection};
    use crate::{
        publish::PublishBytes,
        test_support::{
            decode, drive, encode, handshake, lock_globals, run, FakeBroker, BUILTIN_SUBSCRIPTIONS,
            TEST_DEVICE_ID,
        },
        Error, MqttMessage, Publishable, Topic, DATA_CHANNEL,
    };

    type LastWill = PublishBytes<'static, &'static str, &'static [u8]>;

    static WILL_TOPIC: Topic<&'static str> = Topic::Device("status");

    fn connection<const S: usize>(
        subscriptions: [Topic<&'static str>; S],
    ) -> Connection<'static, &'static str, LastWill, S> {
        Connection {
            last_will: None,
            username: None,
            password: None,
            subscriptions,
        }
    }

    fn pid(n: u16) -> Pid {
        Pid::try_from(n).unwrap()
    }

    fn publish_packet<'a>(topic: &'a str, payload: &'a [u8], qospid: QosPid) -> Packet<'a> {
        Packet::Publish(Publish {
            dup: false,
            qospid,
            retain: false,
            topic_name: topic,
            payload,
        })
    }

    #[test]
    fn packet_sizes() {
        assert_eq!(packet_size(&[]), None);
        assert_eq!(packet_size(&[0x30]), None);
        assert_eq!(packet_size(&[0xc0, 0x00]), Some(2));
        assert_eq!(packet_size(&[0x30, 0x7f]), Some(129));
        // Multi-byte remaining length.
        assert_eq!(packet_size(&[0x30, 0x80]), None);
        assert_eq!(packet_size(&[0x30, 0x80, 0x01]), Some(131));
        assert_eq!(packet_size(&[0x30, 0xff, 0x7f]), Some(16386));
        assert_eq!(packet_size(&[0x30, 0xff, 0xff, 0x7f]), Some(2_097_155));
        assert_eq!(
            packet_size(&[0x30, 0xff, 0xff, 0xff, 0x7f]),
            Some(268_435_460)
        );
        // More than four length bytes is invalid.
        assert_eq!(packet_size(&[0x30, 0xff, 0xff, 0xff, 0xff]), Some(0));

        let encoded = encode(&publish_packet("a/b", &[0; 300], QosPid::AtMostOnce));
        assert_eq!(packet_size(&encoded), Some(encoded.len()));
    }

    #[test]
    fn connect_packet() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection: Connection<'_, &str, LastWill, 0> = Connection {
            last_will: Some(
                WILL_TOPIC
                    .with_bytes(b"offline".as_slice())
                    .qos(QoS::AtLeastOnce)
                    .retain(true),
            ),
            username: Some("user"),
            password: Some("pass"),
            subscriptions: [],
        };

        run(drive(&connection, &broker, async {
            let bytes = broker.receive().await;
            let Packet::Connect(connect) = decode(&bytes) else {
                panic!("expected connect");
            };

            assert_eq!(connect.protocol, Protocol::MQTT311);
            assert_eq!(connect.keep_alive, 60);
            assert_eq!(connect.client_id, TEST_DEVICE_ID);
            assert!(connect.clean_session);
            assert_eq!(connect.username, Some("user"));
            assert_eq!(connect.password, Some(b"pass".as_slice()));

            let will = connect.last_will.unwrap();
            assert_eq!(will.topic, "testdev/0123456789ab/status");
            assert_eq!(will.message, b"offline");
            assert_eq!(will.qos, QoS::AtLeastOnce);
            assert!(will.retain);
        }));
    }

    #[test]
    fn connect_without_credentials() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            let bytes = broker.receive().await;
            let Packet::Connect(connect) = decode(&bytes) else {
                panic!("expected connect");
            };
            assert!(connect.last_will.is_none());
            assert!(connect.username.is_none());
            assert!(connect.password.is_none());
        }));
    }

    #[test]
    fn connack_subscribes() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([Topic::Device("cmd"), Topic::General("global/#")]);

        let topics = run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS + 2).await
        }));

        #[cfg(feature = "homeassistant")]
        assert_eq!(
            topics,
            [
                "homeassistant/status",
                "testdev/0123456789ab/cmd",
                "global/#"
            ]
        );
        #[cfg(not(feature = "homeassistant"))]
        assert_eq!(topics, ["testdev/0123456789ab/cmd", "global/#"]);
    }

    #[test]
    fn connack_rejected() {
        let _lock = lock_globals();
        let bytes = encode(&Packet::Connack(Connack {
            session_present: false,
            code: ConnectReturnCode::NotAuthorized,
        }));

        let result = run(connection([]).recv_loop(bytes.as_slice()));
        assert!(matches!(result, Err(Error::IOError)));
    }

    #[test]
    fn socket_closed() {
        let _lock = lock_globals();
        let result = run(connection([]).recv_loop([].as_slice()));
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_packets() {
        let _lock = lock_globals();

        // Remaining length too long.
        let result = run(connection([]).recv_loop([0x30, 0xff, 0xff, 0xff, 0xff].as_slice()));
        assert!(matches!(result, Err(Error::PacketError)));

        // Larger than the receive buffer.
        let result = run(connection([]).recv_loop([0x30, 0x88, 0x27].as_slice()));
        assert!(matches!(result, Err(Error::PacketError)));

        // Reserved packet type.
        let result = run(connection([]).recv_loop([0x00, 0x00].as_slice()));
        assert!(matches!(result, Err(Error::PacketError)));
    }

    #[test]
    fn receive_publish() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            for (path, expected) in [
                ("testdev/0123456789ab/cmd", Topic::Device("cmd")),
                ("testdev/cmd", Topic::DeviceType("cmd")),
                ("other/cmd", Topic::General("other/cmd")),
            ] {
                broker
                    .send(&publish_packet(path, b"data", QosPid::AtMostOnce))
                    .await;

                let MqttMessage::Publish(topic, payload) = DATA_CHANNEL.receive().await else {
                    panic!("expected publish");
                };
                assert!(topic == expected);
                assert_eq!(&*payload, b"data");
            }
        }));
    }

    #[test]
    fn receive_publish_qos() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            // QoS 1 is acknowledged.
            broker
                .send(&publish_packet("a", b"1", QosPid::AtLeastOnce(pid(7))))
                .await;
            assert!(matches!(
                DATA_CHANNEL.receive().await,
                MqttMessage::Publish(..)
            ));
            assert_eq!(decode(&broker.receive().await), Packet::Puback(pid(7)));

            // QoS 2 goes through the full handshake.
            broker
                .send(&publish_packet("a", b"2", QosPid::ExactlyOnce(pid(8))))
                .await;
            assert!(matches!(
                DATA_CHANNEL.receive().await,
                MqttMessage::Publish(..)
            ));
            assert_eq!(decode(&broker.receive().await), Packet::Pubrec(pid(8)));
            broker.send(&Packet::Pubrel(pid(8))).await;
            assert_eq!(decode(&broker.receive().await), Packet::Pubcomp(pid(8)));
        }));
    }

    #[test]
    fn receive_oversized_publish() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            // Too large for a payload so it is dropped, but still acknowledged.
            let big = [0_u8; 3000];
            broker
                .send(&publish_packet("a", &big, QosPid::AtLeastOnce(pid(3))))
                .await;
            assert_eq!(decode(&broker.receive().await), Packet::Puback(pid(3)));
            assert!(DATA_CHANNEL.try_receive().is_err());
        }));
    }

    #[test]
    fn fragmented_and_batched_packets() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            // A packet split across several reads.
            let bytes = encode(&publish_packet("split", b"payload", QosPid::AtMostOnce));
            for chunk in bytes.chunks(3) {
                broker.send_raw(chunk).await;
                Timer::after_millis(5).await;
            }
            let MqttMessage::Publish(topic, payload) = DATA_CHANNEL.receive().await else {
                panic!("expected publish");
            };
            assert!(topic == Topic::General("split"));
            assert_eq!(&*payload, b"payload");

            // Several packets in a single read, with a trailing partial packet.
            let mut bytes = encode(&publish_packet("one", b"1", QosPid::AtMostOnce));
            bytes.extend(encode(&publish_packet("two", b"2", QosPid::AtMostOnce)));
            let three = encode(&publish_packet("three", b"3", QosPid::AtMostOnce));
            bytes.extend(&three[..4]);
            broker.send_raw(&bytes).await;
            Timer::after_millis(5).await;
            broker.send_raw(&three[4..]).await;

            for expected in ["one", "two", "three"] {
                let MqttMessage::Publish(topic, _) = DATA_CHANNEL.receive().await else {
                    panic!("expected publish");
                };
                assert!(topic == Topic::General(expected));
            }
        }));
    }

    #[test]
    fn publish_qos0() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            Topic::Device("state")
                .with_bytes(b"on")
                .retain(true)
                .publish()
                .await
                .unwrap();

            let bytes = broker.receive().await;
            let Packet::Publish(publish) = decode(&bytes) else {
                panic!("expected publish");
            };
            assert_eq!(publish.topic_name, "testdev/0123456789ab/state");
            assert_eq!(publish.payload, b"on");
            assert_eq!(publish.qospid, QosPid::AtMostOnce);
            assert!(publish.retain);
            assert!(!publish.dup);
        }));
    }

    #[test]
    fn publish_qos1() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            let (result, _) = join(
                Topic::General("t")
                    .with_display(5)
                    .qos(QoS::AtLeastOnce)
                    .publish(),
                async {
                    let bytes = broker.receive().await;
                    let Packet::Publish(publish) = decode(&bytes) else {
                        panic!("expected publish");
                    };
                    assert_eq!(publish.payload, b"5");
                    let QosPid::AtLeastOnce(pid) = publish.qospid else {
                        panic!("expected QoS 1");
                    };
                    // An unrelated acknowledgement is ignored.
                    broker.send(&Packet::Puback(pid + 100)).await;
                    broker.send(&Packet::Puback(pid)).await;
                },
            )
            .await;

            result.unwrap();
        }));
    }

    #[test]
    fn publish_qos2() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            let (result, _) = join(
                Topic::General("t")
                    .with_bytes(b"x")
                    .qos(QoS::ExactlyOnce)
                    .publish(),
                async {
                    let bytes = broker.receive().await;
                    let Packet::Publish(publish) = decode(&bytes) else {
                        panic!("expected publish");
                    };
                    let QosPid::ExactlyOnce(pid) = publish.qospid else {
                        panic!("expected QoS 2");
                    };
                    broker.send(&Packet::Pubrec(pid)).await;
                    assert_eq!(decode(&broker.receive().await), Packet::Pubrel(pid));
                    broker.send(&Packet::Pubcomp(pid)).await;
                },
            )
            .await;

            result.unwrap();
        }));
    }

    #[test]
    fn publish_timeout() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            let result = Topic::General("t")
                .with_bytes(b"x")
                .qos(QoS::AtLeastOnce)
                .publish()
                .await;
            assert!(matches!(result, Err(Error::TimedOut)));
        }));
    }

    #[test]
    fn publish_without_connection() {
        let _lock = lock_globals();
        // With nothing connected the message is silently dropped.
        run(Topic::General("t").with_bytes(b"x").publish()).unwrap();
    }

    async fn ack_subscribe(broker: &FakeBroker, code: SubscribeReturnCodes) -> std::string::String {
        let bytes = broker.receive().await;
        let Packet::Subscribe(subscribe) = decode(&bytes) else {
            panic!("expected subscribe");
        };
        let mut return_codes = heapless::Vec::new();
        return_codes.push(code).unwrap();
        broker
            .send(&Packet::Suback(Suback {
                pid: subscribe.pid,
                return_codes,
            }))
            .await;
        subscribe.topics[0].topic_path.as_str().into()
    }

    #[test]
    fn subscribe() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            let (result, topic) = join(
                Topic::DeviceType("cmd").subscribe(true),
                ack_subscribe(&broker, SubscribeReturnCodes::Success(QoS::AtLeastOnce)),
            )
            .await;
            result.unwrap();
            assert_eq!(topic, "testdev/cmd");

            let (result, _) = join(
                Topic::General("denied").subscribe(true),
                ack_subscribe(&broker, SubscribeReturnCodes::Failure),
            )
            .await;
            assert!(matches!(result, Err(Error::IOError)));

            // Without waiting for an acknowledgement.
            Topic::General("noack").subscribe(false).await.unwrap();
            let bytes = broker.receive().await;
            assert!(matches!(decode(&bytes), Packet::Subscribe(_)));
        }));
    }

    #[test]
    fn unsubscribe() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            let (result, _) = join(Topic::Device("cmd").unsubscribe(true), async {
                let bytes = broker.receive().await;
                let Packet::Unsubscribe(unsubscribe) = decode(&bytes) else {
                    panic!("expected unsubscribe");
                };
                assert_eq!(unsubscribe.topics[0], "testdev/0123456789ab/cmd");
                broker.send(&Packet::Unsuback(unsubscribe.pid)).await;
            })
            .await;
            result.unwrap();
        }));
    }

    #[cfg(feature = "homeassistant")]
    #[test]
    fn home_assistant_status() {
        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            broker
                .send(&publish_packet(
                    "homeassistant/status",
                    b"offline",
                    QosPid::AtMostOnce,
                ))
                .await;
            broker
                .send(&publish_packet(
                    "homeassistant/status",
                    b"online",
                    QosPid::AtMostOnce,
                ))
                .await;

            // Only the online message is reported and neither is forwarded.
            assert!(matches!(
                DATA_CHANNEL.receive().await,
                MqttMessage::HomeAssistantOnline
            ));
            Timer::after_millis(10).await;
            assert!(DATA_CHANNEL.try_receive().is_err());
        }));
    }

    #[cfg(feature = "homeassistant")]
    #[test]
    fn home_assistant_discovery() {
        use crate::homeassistant::{
            binary_sensor::{BinarySensor, BinarySensorState},
            AvailabilityTopics, Device, Entity, Origin,
        };

        let _lock = lock_globals();
        let broker = FakeBroker::new();
        let connection = connection([]);

        let entity = Entity {
            device: Device::new(),
            origin: Origin::new(),
            object_id: "door",
            unique_id: None,
            name: "Door",
            availability: AvailabilityTopics::<0>::None,
            state_topic: Some(Topic::Device("door")),
            command_topic: None,
            component: BinarySensor { device_class: None },
        };

        run(drive(&connection, &broker, async {
            handshake(&broker, BUILTIN_SUBSCRIPTIONS).await;

            entity.publish_discovery().await.unwrap();
            let bytes = broker.receive().await;
            let Packet::Publish(publish) = decode(&bytes) else {
                panic!("expected publish");
            };
            assert_eq!(
                publish.topic_name,
                "homeassistant/binary_sensor/door/config"
            );
            assert!(publish.payload.starts_with(br#"{"dev":"#));

            entity.publish_state(BinarySensorState::On).await.unwrap();
            let bytes = broker.receive().await;
            let Packet::Publish(publish) = decode(&bytes) else {
                panic!("expected publish");
            };
            assert_eq!(publish.topic_name, "testdev/0123456789ab/door");
            assert_eq!(publish.payload, b"ON");
        }));
    }
}
