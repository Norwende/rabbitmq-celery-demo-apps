"""Entry point: ``python -m subscriber``."""

import logging
import signal
from functools import partial

from kombu import Connection

from subscriber.config import Settings
from subscriber.consumer import STREAM, StreamSubscriber
from subscriber.handlers import make_handler
from subscriber.offsets import Checkpoint, parse_offset_spec
from subscriber.redis_client import connect_redis

logger = logging.getLogger("subscriber")


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format=f"%(asctime)s %(levelname)-7s [{settings.subscriber_name}] %(name)s: %(message)s",
    )
    offset_spec = parse_offset_spec(settings.stream_offset)
    redis_factory = partial(connect_redis, settings)
    handler = make_handler(settings.subscriber_role, redis_factory)
    checkpoint = Checkpoint(redis_factory(), STREAM, settings.subscriber_name) if settings.checkpoint_enabled else None

    with Connection(settings.broker_url, heartbeat=30) as connection:
        subscriber = StreamSubscriber(connection, handler, offset_spec, settings.prefetch_count, checkpoint)

        def stop(signum, frame) -> None:
            logger.info("received %s, stopping", signal.Signals(signum).name)
            subscriber.should_stop = True

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        logger.info("starting subscriber name=%s role=%s offset=%s checkpoint=%s",
                    settings.subscriber_name, settings.subscriber_role, offset_spec,
                    checkpoint.key if checkpoint else "disabled")
        subscriber.run()
    logger.info("stopped")


if __name__ == "__main__":
    main()
