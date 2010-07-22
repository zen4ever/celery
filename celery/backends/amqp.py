"""celery.backends.amqp"""
import socket
import time

from datetime import timedelta

from kombu.entity import Exchange, Binding
from kombu.messaging import Consumer, Producer

from celery import conf
from celery import states
from celery.backends.base import BaseDictBackend
from celery.exceptions import TimeoutError
from celery.messaging import establish_connection
from celery.utils import timeutils


class AMQPBackend(BaseDictBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    _connection = None
    _channel = None

    def __init__(self, connection=None, exchange=None, exchange_type=None,
            persistent=None, serializer=None, auto_delete=True,
            expires=None, **kwargs):
        self._connection = connection
        exchange = exchange or conf.RESULT_EXCHANGE
        exchange_type = exchange_type or conf.RESULT_EXCHANGE_TYPE
        if persistent is None:
            persistent = conf.RESULT_PERSISTENT
        delivery_mode = persistent and "persistent" or "transient"
        self.exchange = Exchange(name=exchange,
                                 type=exchange_type,
                                 delivery_mode=delivery_mode,
                                 auto_delete=auto_delete)
        self.persistent = persistent
        self.auto_delete = auto_delete
        self.expires = expires
        self.serializer = serializer or conf.RESULT_SERIALIZER
        if self.expires is None:
            self.expires = conf.TASK_RESULT_EXPIRES
        if isinstance(self.expires, timedelta):
            self.expires = timeutils.timedelta_seconds(self.expires)
        if self.expires is not None:
            self.expires = int(self.expires)

        super(AMQPBackend, self).__init__(**kwargs)

    def _create_binding(self, task_id):
        name = task_id.replace("-", "")
        return Binding(name=name,
                       exchange=self.exchange,
                       routing_key=name,
                       durable=self.persistent,
                       auto_delete=self.auto_delete)

    def _create_producer(self, task_id):
        binding = self._create_binding(task_id)
        binding(self.channel).declare()

        return Producer(self.channel, exchange=self.exchange,
                        routing_key=task_id.replace("-", ""),
                        serializer=self.serializer)

    def _create_consumer(self, task_id):
        binding = self._create_binding(task_id)
        return Consumer(self.channel, binding, no_ack=True)

    def store_result(self, task_id, result, status, traceback=None):
        """Send task return value and status."""
        result = self.encode_result(result, status)

        meta = {"task_id": task_id,
                "result": result,
                "status": status,
                "traceback": traceback}

        self._create_producer(task_id).publish(meta)

        return result

    def get_task_meta(self, task_id, cache=True):
        return self.poll(task_id)

    def wait_for(self, task_id, timeout=None, cache=True):
        if task_id in self._cache:
            meta = self._cache[task_id]
        else:
            try:
                meta = self.consume(task_id, timeout=timeout)
            except socket.timeout:
                raise TimeoutError("The operation timed out.")

        if meta["status"] == states.SUCCESS:
            return meta["result"]
        elif meta["status"] in states.PROPAGATE_STATES:
            raise self.exception_to_python(meta["result"])

    def poll(self, task_id):
        binding = self._create_binding(task_id)(self.channel)
        result = binding.get()
        if result:
            payload = self._cache[task_id] = result.payload
            return payload
        else:

            # Use previously received status if any.
            if task_id in self._cache:
                return self._cache[task_id]

            return {"status": states.PENDING, "result": None}

    def consume(self, task_id, timeout=None):
        results = []

        def callback(message_data, message):
            results.append(message_data)

        wait = self.connection.drain_events
        consumer = self._create_consumer(task_id)
        consumer.register_callback(callback)

        consumer.consume()
        try:
            time_start = time.time()
            while True:
                # Total time spent may exceed a single call to wait()
                if timeout and time.time() - time_start >= timeout:
                    raise socket.timeout()
                wait(timeout=timeout)
                if results:
                    # Got event on the wanted channel.
                    break
        finally:
            consumer.cancel()

        self._cache[task_id] = results[0]
        return results[0]

    def close(self):
        if self._channel is not None:
            self._channel.close()
        if self._connection is not None:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = establish_connection()
        return self._connection

    @property
    def channel(self):
        if not self._channel:
            self._channel = self.connection.channel()
        return self._channel

    def reload_task_result(self, task_id):
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")

    def save_taskset(self, taskset_id, result):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "save_taskset is not supported by this backend.")

    def restore_taskset(self, taskset_id, cache=True):
        """Get the result of a taskset."""
        raise NotImplementedError(
                "restore_taskset is not supported by this backend.")
