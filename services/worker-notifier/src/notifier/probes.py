"""File-based Kubernetes probes, cheap enough to avoid ``celery inspect``.

* readiness: ``/tmp/celery-ready`` exists between ``worker_ready`` and shutdown;
* liveness: ``/tmp/celery-heartbeat`` is touched every few seconds by a timer running in
  the worker's event loop, so it goes stale if the main loop hangs.
"""

from pathlib import Path

from celery import Celery, bootsteps
from celery.signals import worker_ready, worker_shutdown

READY_FILE = Path("/tmp/celery-ready")
HEARTBEAT_FILE = Path("/tmp/celery-heartbeat")
HEARTBEAT_INTERVAL_SECONDS = 10.0


class LivenessHeartbeat(bootsteps.StartStopStep):
    requires = frozenset({"celery.worker.components:Timer"})

    def __init__(self, worker, **kwargs):
        super().__init__(worker, **kwargs)
        self.tref = None

    def start(self, worker) -> None:
        HEARTBEAT_FILE.touch()
        self.tref = worker.timer.call_repeatedly(HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_FILE.touch)

    def stop(self, worker) -> None:
        if self.tref is not None:
            self.tref.cancel()
        HEARTBEAT_FILE.unlink(missing_ok=True)


def _mark_ready(**kwargs) -> None:
    READY_FILE.touch()


def _mark_not_ready(**kwargs) -> None:
    READY_FILE.unlink(missing_ok=True)


def install_probes(app: Celery) -> None:
    app.steps["worker"].add(LivenessHeartbeat)
    worker_ready.connect(_mark_ready, weak=False)
    worker_shutdown.connect(_mark_not_ready, weak=False)
