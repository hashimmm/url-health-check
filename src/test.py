from typing import final
import unittest
from time import monotonic
from health import check_health, get_metrics_for_last_5_mins
import db
import producer
import consumer
import settings


class MonitorTestCase(unittest.TestCase):
    def test_check_health(self):
        health = check_health("https://example.com/", timeout_secs=1)
        self.assertEqual(health.status, "ok")
        self.assertEqual(health.code, 200)
        self.assertGreater(health.time_taken, 0)

    def test_check_health_negative(self):
        health = check_health("https://example.com/xyz", timeout_secs=1)
        self.assertEqual(health.status, "bad")
        self.assertEqual(health.code, 404)
        self.assertGreater(health.time_taken, 0)

    def test_check_health_timeout(self):
        health = check_health("https://example.com/xyz", timeout_secs=0.0001)
        self.assertEqual(health.status, "timeout")
        self.assertIsNone(health.code)
        self.assertIsNone(health.time_taken)


class ProducerConsumerTestCase(unittest.TestCase):
    def test_flow(self):
        p = producer.make_producer()
        health = check_health("https://example.com/")
        producer.send_to_kafka(health, p, topic=settings.TEST_KAFKA_TOPIC)
        p.flush()
        start = monotonic()
        # find our message for ten seconds.
        returned_health = None
        c = consumer.make_consumer(topic=settings.TEST_KAFKA_TOPIC)
        try:
            while (monotonic() - start) < 30:
                returned_health = consumer.hc_consume(c)
                if returned_health is not None:
                    break
        finally:
            c.close()
        self.assertEqual(health, returned_health)
        db.ensure_table(connection_uri=settings.TEST_POSTGRES_URI)
        consumer.store_health(health, settings.TEST_POSTGRES_URI)
        # This is basically a sanity check. The calculation is done inside postgres, so if we record the right thing
        # we should get the right answers. As long as this doesn't error out, we're likely doing alright.
        avg, num_bad, ninetieth_pc = get_metrics_for_last_5_mins("https://example.com/", settings.TEST_POSTGRES_URI)


if __name__ == "__main__":
    unittest.main()
