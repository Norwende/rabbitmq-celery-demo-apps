"""Liveness of the beat loop, cheap enough to avoid ``celery inspect``.

Beat's main loop calls ``Scheduler.tick()`` at least every ``beat_max_loop_interval``
seconds (10 s here), also when nothing is due. Touching a file there makes it go stale if
the loop hangs (e.g. blocked on a broker reconnection), which the liveness probe detects.
"""

from pathlib import Path

from celery.beat import PersistentScheduler

HEARTBEAT_FILE = Path("/tmp/beat-heartbeat")


class HeartbeatScheduler(PersistentScheduler):
    """``PersistentScheduler`` (shelve file) that touches ``HEARTBEAT_FILE`` on every tick."""

    def tick(self, *args, **kwargs):
        HEARTBEAT_FILE.touch()
        return super().tick(*args, **kwargs)
