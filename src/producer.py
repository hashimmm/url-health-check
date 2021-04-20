from time import sleep
from health import Health, check_health
from confluent_kafka import Producer
from settings import (
    KAFKA_TOPIC,
    KAFKA_HOST,
    KAFKA_PORT,
    KAFKA_ACCESS_KEY,
    KAFKA_CERT,
    KAFKA_CA_CERT,
    PRODUCER_DELAY_SECS,
)


# From the confluent docs
def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        pass
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def send_to_kafka(health_data: Health, producer: Producer, topic: str = KAFKA_TOPIC):
    """Helper function so we can send `Health` records to kafka."""
    producer.produce(
        topic, health_data.to_json().encode("utf-8"), callback=delivery_report
    )


def hc_produce_forever(
    producer: Producer,
    delay_seconds: float = PRODUCER_DELAY_SECS,
    url: str = "https://www.example.com/",
    topic=KAFKA_TOPIC,
):
    """Loop forever, checking health and sending to kafka."""
    while True:
        health = check_health(url)
        # Trigger any available delivery report callbacks from previous produce() calls
        producer.poll(0)
        send_to_kafka(health, producer, topic=topic)
        sleep(delay_seconds)


def make_producer():
    with open("cafile", "w") as f:
        f.write(KAFKA_CA_CERT)
    return Producer(
        {
            "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
            "security.protocol": "ssl",
            "ssl.key.pem": KAFKA_ACCESS_KEY,
            "ssl.certificate.pem": KAFKA_CERT,
            "ssl.ca.location": "cafile",
        }
    )


if __name__ == "__main__":
    p = make_producer()
    try:
        hc_produce_forever(p)
    except KeyboardInterrupt:
        p.flush()
