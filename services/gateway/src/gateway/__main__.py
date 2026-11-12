"""Entry point: ``python -m gateway``."""

import logging

import uvicorn

from gateway.config import settings


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    # log_config=None: uvicorn's loggers propagate to the root configuration above.
    uvicorn.run("gateway.main:app", host="0.0.0.0", port=8000, log_config=None)


if __name__ == "__main__":
    main()
