from unittest import TestCase
from easyrabbit import RoutingReader, RoutingWriter


class TestBasicRouting(TestCase):
    def test_both(self):
        url = 'amqp://localhost'
        exchange = 'TEST_EXCHANGE'
        queue_name = 'test_queue'
        routing_key = 'test_rtkey'

        msgs = {b'hello', b'world'}

        with RoutingReader(url, exchange, queue_name, routing_key) as reader:
            with RoutingWriter(url, exchange, routing_key) as writer:
                for msg in msgs:
                    writer.put(msg)

            for msg, res in zip(msgs, reader):
                self.assertEqual(msg, res)
