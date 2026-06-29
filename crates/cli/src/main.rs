use bus::Bus;
use message_core::Message;

fn main() {
    let b = Bus::new(1024);
    let (_id, rx) = b.subscribe("demo");

    b.publish(Message::new("demo", b"hello from cadence"));

    match rx.try_recv() {
        Ok(m) => println!(
            "[{}ns] {} => {:?}",
            m.timestamp_ns,
            m.topic_str(),
            String::from_utf8_lossy(m.payload_bytes())
        ),
        Err(e) => eprintln!("recv error: {e}"),
    }
}
