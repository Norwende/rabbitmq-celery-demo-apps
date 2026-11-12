"""Plain kombu consumer of the ``orders.events`` stream over AMQP 0.9.1.

Stream consumption over AMQP 0.9.1 requires a consumer prefetch (``basic.qos`` with
``global=False``), manual acknowledgements (they only grant credit, nothing is deleted)
and the ``x-stream-offset`` consumer argument to choose where to attach.
"""

import logging
import time
from pathlib import Path

from kombu import Connection, Exchange, Message, Queue
from kombu.mixins import ConsumerMixin

from subscriber.handlers import Handler
from subscriber.offsets import Checkpoint, resolve_start_offset

logger = logging.getLogger(__name__)

STREAM = "orders.events"
ORDERS_EXCHANGE = Exchange("orders", type="topic", durable=True)
STREAM_ARGUMENTS = {"x-queue-type": "stream", "x-max-age": "7D"}
HEARTBEAT_FILE = Path("/tmp/subscriber-heartbeat")
HEARTBEAT_INTERVAL_SECONDS = 5.0


class StreamSubscriber(ConsumerMixin):
    def __init__(
        self,
        connection: Connection,
        handler: Handler,
        offset_spec: str | int,
        prefetch_count: int,
        checkpoint: Checkpoint | None,
    ) -> None:
        self.connection = connection
        self.handler = handler
        self.offset_spec = offset_spec
        self.prefetch_count = prefetch_count
        self.checkpoint = checkpoint
        # Survives reconnections: we re-attach right after the last processed message.
        self.last_offset = checkpoint.load() if checkpoint else None
        self._last_heartbeat = 0.0

    def get_consumers(self, Consumer, channel):
        start = resolve_start_offset(self.offset_spec, self.last_offset)
        logger.info("attaching to stream %s at offset=%s (last processed=%s)", STREAM, start, self.last_offset)
        queue = Queue(
            STREAM,
            exchange=ORDERS_EXCHANGE,
            routing_key="order.#",
            durable=True,
            queue_arguments=STREAM_ARGUMENTS,
            consumer_arguments={"x-stream-offset": start},
        )
        # kombu applies prefetch_count with basic.qos(global=False) before basic.consume.
        return [Consumer(queues=[queue], callbacks=[self.on_message], accept=["json"],
                         prefetch_count=self.prefetch_count, no_ack=False)]

    def on_message(self, body: dict, message: Message) -> None:
        offset = message.headers["x-stream-offset"]
        # Delivery starts at the beginning of the chunk holding the requested offset, so
        # messages already processed may come again after a re-attach: skip them.
        if self.last_offset is not None and offset <= self.last_offset:
            logger.debug("skipping already processed offset=%s", offset)
            message.ack()
            return
        self.handler(body, offset)
        if self.checkpoint:
            self.checkpoint.save(offset)
        message.ack()
        self.last_offset = offset

    def on_consume_ready(self, connection, channel, consumers, **kwargs) -> None:
        logger.info("consuming from %s", STREAM)
        self._touch_heartbeat()

    def on_connection_error(self, exc, interval) -> None:
        logger.warning("broker connection error: %s, retrying in %ss", exc, interval)

    def on_iteration(self) -> None:
        if time.monotonic() - self._last_heartbeat >= HEARTBEAT_INTERVAL_SECONDS:
            self._touch_heartbeat()

    def _touch_heartbeat(self) -> None:
        HEARTBEAT_FILE.touch()
        self._last_heartbeat = time.monotonic()
