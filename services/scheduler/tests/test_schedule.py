from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from celery.schedules import crontab

from scheduler.celery_app import DIGEST_TASK, app, build_beat_schedule, route_by_prefix
from scheduler.config import Settings
from scheduler.probes import HeartbeatScheduler


def settings(**env) -> Settings:
    return replace(Settings.from_env(), **env)


def test_default_schedule_sends_the_digest_by_name_every_minute_with_expiry():
    entry = build_beat_schedule(settings())["sales-digest"]
    assert entry["task"] == DIGEST_TASK == "notifications.sales_digest"
    assert entry["schedule"] == crontab.from_string("* * * * *")
    assert entry["options"] == {"expires": 50}
    assert "args" not in entry and "kwargs" not in entry


def test_schedule_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("DIGEST_CRON", "*/5 8-18 * * 1-5")
    monkeypatch.setenv("DIGEST_EXPIRES_SECONDS", "30")
    entry = build_beat_schedule(Settings.from_env())["sales-digest"]
    assert entry["schedule"] == crontab(minute="*/5", hour="8-18", day_of_week="1-5")
    assert entry["options"] == {"expires": 30}


@pytest.mark.parametrize("cron", ["* * * *", "61 * * * *", "every minute"])
def test_invalid_cron_is_rejected(cron):
    with pytest.raises(ValueError):
        build_beat_schedule(settings(digest_cron=cron))


@pytest.mark.parametrize("expires", [0, 60, 3600])
def test_expiry_must_stay_within_the_minute_of_the_tick(expires):
    with pytest.raises(ValueError):
        build_beat_schedule(settings(digest_expires_seconds=expires))


def test_digest_is_routed_to_the_notifications_queue():
    assert route_by_prefix(DIGEST_TASK, (), {}, {}) == {"queue": "notifications"}
    assert route_by_prefix("celery.backend_cleanup", (), {}, {}) is None


def test_queue_declaration_block_matches_the_contract():
    conf = app.conf
    assert (conf.task_default_queue_type, conf.task_create_missing_queue_type) == ("quorum", "quorum")
    assert (conf.task_default_exchange_type, conf.task_create_missing_queue_exchange_type) == ("topic", "topic")
    assert conf.broker_transport_options == {"confirm_publish": True}
    assert conf.accept_content == ["json"] and conf.task_serializer == "json"


def test_beat_runs_only_the_configured_entry(tmp_path):
    scheduler = HeartbeatScheduler(app=app, schedule_filename=str(tmp_path / "schedule"), lazy=True)
    scheduler.setup_schedule()
    try:
        # No "celery.backend_cleanup" default entry (result_expires=None, no backend).
        assert set(scheduler.schedule) == {"sales-digest"}
    finally:
        scheduler.close()


def every_minute_at(now: datetime) -> crontab:
    return crontab(minute="*", app=app, nowfun=lambda: now)


def test_missed_slot_is_not_caught_up_after_a_restart():
    """A slot missed for longer than beat_cron_starting_deadline (30 s) is dropped."""
    now = datetime(2026, 11, 11, 21, 10, 45, tzinfo=UTC)
    stale_last_run = now - timedelta(minutes=10)  # "last run" kept in the schedule file
    assert every_minute_at(now).is_due(stale_last_run) == (False, 15.0)


def test_at_most_the_current_slot_runs_after_a_restart():
    """10 missed slots: only the current one (21:10:00, 5 s late) runs, once."""
    now = datetime(2026, 11, 11, 21, 10, 5, tzinfo=UTC)
    due, next_in = every_minute_at(now).is_due(now - timedelta(minutes=10))
    assert (due, next_in) == (True, 55.0)


def test_tick_touches_the_heartbeat_file(tmp_path, monkeypatch):
    heartbeat = tmp_path / "beat-heartbeat"
    monkeypatch.setattr("scheduler.probes.HEARTBEAT_FILE", heartbeat)
    monkeypatch.setattr(HeartbeatScheduler, "apply_entry", lambda self, entry, producer=None: None)
    scheduler = HeartbeatScheduler(app=app, schedule_filename=str(tmp_path / "schedule"), lazy=True)
    scheduler.setup_schedule()
    try:
        interval = scheduler.tick()
    finally:
        scheduler.close()
    assert heartbeat.exists()
    assert 0 < interval <= app.conf.beat_max_loop_interval
