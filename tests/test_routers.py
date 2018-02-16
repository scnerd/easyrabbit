from unittest import TestCase
from easyrabbit import BlockingReader, BlockingWriter


class TestBasicRouting(TestCase):
    def test_both(self):
        url = 'localhost'
        exchange = 'TEST_EXCHANGE'
        queue_name = 'test_queue'
        routing_key = 'test_rtkey'

        msgs = {b'hello', b'world'}

        with BlockingReader(url, exchange, queue_name, routing_key) as reader:
            with BlockingWriter(url, exchange, routing_key) as writer:
                for msg in msgs:
                    writer.put(msg)

            for msg, res in zip(msgs, reader):
                self.assertEqual(msg, res)
