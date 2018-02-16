import pika
from queue import Empty, Queue
import multiprocessing as mp
import ctypes
import time
import math
import logging
import os
import signal
from datetime import datetime

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)

WAIT_READY_TIMEOUT = None


def _fmt_bytes(b, maxlen=32):
    if len(b) > maxlen:
        return "{}...({} bytes)".format(b[:maxlen], len(b))
    else:
        return "{}({} bytes)".format(b, len(b))


class _RoutingConnector:
    def __init__(self, url, exchange, daemon=True, exchange_arguments={}):
        self._params = pika.URLParameters(url)
        self._exchange = exchange
        self._exchange_args = exchange_arguments or {}
        self._pipe_in, self._pipe_out = mp.Pipe()
        self._connection = None
        self._channel = None
        self._ready = mp.Value(ctypes.c_bool, False)
        # self._counter = mp.Value(ctypes.c_int, 0)
        # self._starttime = datetime.now()

        self._proc = mp.Process(target=self._run, daemon=daemon)

    @property
    def child_pipe(self):
        raise NotImplementedError()

    @property
    def parent_pipe(self):
        raise NotImplementedError()

    def _run(self):
        self.parent_pipe.close()

        log.debug("{} creating connection".format(self))
        self._connection = pika.SelectConnection(self._params,
                                                 on_open_callback=self._on_open,
                                                 stop_ioloop_on_close=True)

        try:
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            log.debug("{} received interrupt and is exiting".format(self))

    def _on_open(self, connection):
        connection.channel(self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        self._channel.exchange_declare(self._on_exchange_ok, self._exchange, arguments=self._exchange_args)

    def _on_exchange_ok(self, _):
        raise NotImplementedError()

    def _mark_ready(self):
        self._ready.value = True

    def wait_till_ready(self, timeout=None, interval=0.001):
        timeout = timeout if timeout is not None else WAIT_READY_TIMEOUT
        starttime = datetime.now()
        if timeout is not None and timeout > 0:
            for _ in range(int(math.ceil(timeout / interval))):
                if self._ready.value:
                    return True
                time.sleep(interval)
                log.debug("Waiting on {} to be ready for {}s".format(self, (datetime.now() - starttime).total_seconds()))
        else:
            while not self._ready.value:
                time.sleep(interval)
                log.debug("Waiting on {} to be ready for {}s".format(self, (datetime.now() - starttime).total_seconds()))
            return True

        raise TimeoutError()

    def _interrupt(self):
        log.debug("{} terminating via interrupt".format(self))
        os.kill(self._proc.pid, signal.SIGINT)

    def start(self):
        self._proc.start()
        self.child_pipe.close()

    def close(self):
        try:
            self._interrupt()
            self.parent_pipe.close()
        except ProcessLookupError:
            # The process is already toast
            log.debug("Attempted to close {} but the child process was already gone".format(self))
        self._proc.join()

        # secs = (datetime.now() - self._starttime).total_seconds()
        # count = self._counter.value
        # log.info("{} processed {} messages over {} seconds ({}/s)".format(self, count, secs, count / secs))

    def __enter__(self):
        self.start()
        self.wait_till_ready()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class RoutingReader(_RoutingConnector):
    def __init__(self, url, exchange, queue_name, routing_key, *,
                 exclusive=False, exchange_args=None, queue_args=None, daemon=True):
        """

        :param url:
        :type url: str
        :param exchange:
        :type exchange: str
        :param queue_name:
        :type queue_name: str
        :param routing_key:
        :type routing_key: str
        :param exclusive:
        :type exclusive: bool
        :param daemon:
        :type daemon: bool
        :param exchange_args:
        :type exchange_args: dict
        :param queue_args:
        :type queue_args: dict
        """
        self._consumer_tag = None
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._exclusive = exclusive
        self._queue_args = queue_args or {}
        super().__init__(url, exchange, exchange_arguments=exchange_args, daemon=daemon)

    @property
    def child_pipe(self):
        return self._pipe_in

    @property
    def parent_pipe(self):
        return self._pipe_out

    def _on_exchange_ok(self, _):
        self._channel.queue_declare(callback=self._on_queue_ok,
                                    queue=self._queue_name,
                                    exclusive=self._exclusive,
                                    arguments=self._queue_args)

    def _on_queue_ok(self, _):
        self._channel.queue_bind(callback=self._on_bind_ok,
                                 queue=self._queue_name,
                                 exchange=self._exchange,
                                 routing_key=self._routing_key)

    def _on_bind_ok(self, _):
        self._channel.add_on_cancel_callback(self._on_cancel)
        self._start_consuming()

    def _start_consuming(self):
        self._consumer_tag = self._channel.basic_consume(self._on_message, self._queue_name)

        log.debug("{} listening on {}/{} for key {}".format(self, self._exclusive, self._queue_name, self._routing_key))
        self._mark_ready()

    def _on_cancel(self, _):
        self._connection.close()

    def _on_message(self, chan, deliver, props, body):
        try:
            log.debug("{} received {} from {}/{}".format(self, _fmt_bytes(body), self._exchange, self._queue_name))
            self.child_pipe.send_bytes(body)
            self._channel.basic_ack(delivery_tag=deliver.delivery_tag)
        except Exception:
            self._channel.basic_nack(delivery_tag=deliver.delivery_tag)
            raise

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._on_cancel_ok, self._consumer_tag)
        self._connection.ioloop.start()

    def _on_cancel_ok(self, _):
        self._channel.close()
        self._connection.close()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get()
        except BrokenPipeError:
            raise StopIteration()

    def get(self):
        return self.parent_pipe.recv_bytes()

    def get_nowait(self):
        if self.parent_pipe.poll():
            return self.parent_pipe.recv_bytes()
        else:
            raise Empty()

    def getall_nowait(self, max_items=None):
        if max_items:
            for _ in range(max_items):
                yield self.get_nowait()
        else:
            while self.parent_pipe.poll():
                yield self.get()

    def empty(self):
        return not self.parent_pipe.poll()


class RoutingWriter(_RoutingConnector):
    def __init__(self, url, exchange, routing_key, *,
                 mandatory=False, immediate=False, retry=False, poll_time=0.01, exchange_args=None, daemon=True):
        """

        :param url:
        :type url: str
        :param exchange:
        :type exchange: str
        :param routing_key:
        :type routing_key: str
        :param mandatory:
        :type mandatory: bool
        :param immediate:
        :type immediate: bool
        :param retry:
        :type retry: bool
        :param daemon:
        :type daemon: bool
        :param poll_time:
        :type poll_time: Real
        :param exchange_args:
        :type exchange_args: dict
        """
        self._routing_key = routing_key
        self._poll_timeout = poll_time
        self._mandatory = mandatory
        self._immediate = immediate
        self._retry = retry
        self._retry_queue = Queue() if retry else None

        super().__init__(url, exchange, exchange_arguments=exchange_args, daemon=daemon)

    @property
    def child_pipe(self):
        return self._pipe_out

    @property
    def parent_pipe(self):
        return self._pipe_in

    def _on_exchange_ok(self, _):
        self._channel.add_on_return_callback(self._on_return)
        self._mark_ready()
        self._publish()

    def _publish(self):
        # Send new messages
        while self.child_pipe.poll():
            msg = self.child_pipe.recv_bytes()
            self._channel.basic_publish(self._exchange, self._routing_key, msg,
                                        mandatory=self._mandatory, immediate=self._immediate)

        # Send retry messages
        while self._retry_queue and not self._retry_queue.empty():
            self._channel.basic_publish(self._exchange, self._routing_key, self._retry_queue.get(),
                                        mandatory=self._mandatory, immediate=self._immediate)

        # Schedule this function for sometime in the near future
        self._connection.add_timeout(self._poll_timeout, self._publish)

    def _on_return(self, channel, method, prop, body):
        if self._retry:
            log.warning("{} got message {} returned, retrying to send".format(self, _fmt_bytes(body)))
            self._retry_queue.put(body)
        else:
            log.warning("{} got message {} returned, dropping it".format(self, _fmt_bytes(body)))

    def put(self, value):
        self.parent_pipe.send_bytes(value)

    def putall(self, values):
        for v in values:
            self.put(v)
