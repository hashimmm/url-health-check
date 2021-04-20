from datetime import datetime
from typing import TYPE_CHECKING, Union
from time import sleep
import db
import pytz
from confluent_kafka import Consumer
from settings import (
    KAFKA_TOPIC,
    KAFKA_HOST,
    KAFKA_PORT,
    KAFKA_ACCESS_KEY,
    KAFKA_CERT,
    KAFKA_CA_CERT,
    CONSUMER_DELAY_SECS,
    POSTGRES_URI,
)
from health import Health


# See: https://github.com/django/django/blob/aa4acc164d1247c0de515c959f7b09648b57dc42/django/utils/timezone.py#L193
def now_with_tz():
    """Get the time now with time zone."""
    return datetime.utcnow().replace(tzinfo=pytz.utc)


def hc_consume(c) -> Union[Health, None]:
    """Get a health record from the kafka consumer if there is a new one."""
    msg = c.poll(1.0)
    if msg is None:
        return
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        return
    health_record = Health.from_json(msg.value().decode("utf-8"))
    c.commit()
    print(f"Received health record {health_record}")
    return health_record


def make_consumer(topic=KAFKA_TOPIC):
    """Make a kafka consumer."""
    with open("cafile-consumer", "w") as f:
        f.write(KAFKA_CA_CERT)
    c = Consumer(
        {
            "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
            "security.protocol": "ssl",
            "ssl.key.pem": KAFKA_ACCESS_KEY,
            "ssl.certificate.pem": KAFKA_CERT,
            "ssl.ca.location": "cafile-consumer",
            "group.id": "somegroup",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([topic])
    return c


def hc_consume_forever(c, db_connection_uri: str, delay_secs: float = CONSUMER_DELAY_SECS):
    """Consumer health records from kafka and store into db."""
    while True:
        health_record = hc_consume(c)
        if health_record:
            store_health(health_record, db_connection_uri)
        sleep(delay_secs)


def store_health(health_check_record: Health, db_connection_uri: str):
    """Store health record into db."""
    with db.get_cursor(connection_uri=db_connection_uri) as dbh:
        dbh.execute(
            """Insert into health_check_data (
                    recorded_at,
                    url,
                    status_code,
                    status,
                    response_time_secs)
                VALUES (%(recorded_at)s, %(url)s, %(code)s, %(status)s, %(time_taken)s)
            ;""",
            {
                "recorded_at": now_with_tz(),
                "url": health_check_record.url,
                "status": health_check_record.status,
                "code": health_check_record.code,
                "time_taken": health_check_record.time_taken,
            },
        )
        dbh.commit()


if __name__ == "__main__":
    db.ensure_table()
    c = make_consumer(topic=KAFKA_TOPIC)
    try:
        hc_consume_forever(c, db_connection_uri=POSTGRES_URI)
    except KeyboardInterrupt:
        c.close()
        db.close_all()
