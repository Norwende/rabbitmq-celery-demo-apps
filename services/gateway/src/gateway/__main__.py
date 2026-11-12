"""Entry point: ``python -m gateway``."""

import uvicorn

from gateway.config import settings
from gateway.logs import configure_logging


def main() -> None:
    configure_logging(settings.log_level, settings.log_format)
    # log_config=None: uvicorn's loggers propagate to the root handler set up above.
    # access_log=False: RequestContextMiddleware emits one structured access event instead.
    uvicorn.run("gateway.main:app", host="0.0.0.0", port=8000, log_config=None, access_log=False)


if __name__ == "__main__":
    main()
