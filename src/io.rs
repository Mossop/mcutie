use core::ops::Deref;

pub(crate) use atomic16::assign_pid;
use embassy_futures::select::{select, select4, Either};
use embassy_net::{
    dns::DnsQueryType,
    tcp::{TcpReader, TcpSocket, TcpWriter},
    Stack,
};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    pubsub::{PubSubChannel, Subscriber, WaitResult},
};
use embassy_time::Timer;
use embedded_io_async::Write;
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

fn packet_size(buffer: &[u8]) -> Option<usize> {
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
    pub(crate) last_will: Option<L>,
    pub(crate) username: Option<&'t str>,
    pub(crate) password: Option<&'t str>,
    pub(crate) subscriptions: [Topic<T>; S],
}

impl<'t, T, L, const S: usize> McutieTask<'t, T, L, S>
where
    T: Deref<Target = str> + 't,
    L: Publishable + 't,
{
    #[cfg(not(feature = "homeassistant"))]
    async fn ha_handle_update(&self, _topic: &Topic<TopicString>, _payload: &Payload) -> bool {
        false
    }

    async fn recv_loop(&self, mut reader: TcpReader<'_>) -> Result<(), Error> {
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
                    Some(len) => len,
                    None => {
                        // None is returned when there is not yet enough data to decode a packet.
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
                    Packet::Pubrel(pid) => send_packet(Packet::Pubrel(pid)).await?,
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

    async fn write_loop(&self, mut writer: TcpWriter<'_>) {
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
            error!("Failed to send connection packet: {:?}", e);
            return;
        }

        let reader = SEND_QUEUE.reader();

        loop {
            let buffer = reader.receive().await;

            trace!("Writer sending packet");
            if let Err(e) = writer.write_all(&buffer).await {
                error!("Failed to send data: {:?}", e);
                return;
            }
        }
    }

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

            let recv_loop = self.recv_loop(reader);
            let send_loop = self.write_loop(writer);

            let ping_loop = async {
                loop {
                    Timer::after_secs(45).await;

                    let _ = send_packet(Packet::Pingreq).await;
                }
            };

            let link_down = async {
                self.network.wait_link_down().await;
                warn!("Network link lost");
            };

            let ip_down = async {
                self.network.wait_config_down().await;
                warn!("Network config lost");
            };

            select4(send_loop, ping_loop, recv_loop, select(link_down, ip_down)).await;

            socket.close();

            warn!("Lost connection with broker");
            DATA_CHANNEL.send(MqttMessage::Disconnected).await;
        }
    }
}
