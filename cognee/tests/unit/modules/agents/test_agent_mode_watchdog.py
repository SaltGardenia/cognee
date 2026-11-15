"""The agent-mode watchdog escalates when a graceful shutdown hangs (#6018).

One empty tick sends SIGTERM; further empty ticks warn and repeat it for a
grace period; past the grace the process is force-killed. Activity resets the
escalation, so an agent re-registering mid-shutdown is never force-killed.
"""

import os
import signal
import threading

import pytest

from cognee.modules.agents import agent_mode


class _NoRearmTimer:
    """Stands in for threading.Timer so tests drive ticks directly."""

    def __init__(self, interval, function):
        self.interval = interval
        self.function = function
        self.daemon = False

    def start(self):
        pass


@pytest.fixture()
def watchdog_signals(monkeypatch):
    """Drive `_watchdog` by hand and record every signal it sends to itself.

    ``os._exit`` is recorded as a ``(pid, "os._exit")`` marker too: the
    force-kill escalation on platforms without SIGKILL (Windows) exits the
    process, which here must be the test, not the runner.
    """
    sent: list[tuple[int, object]] = []
    monkeypatch.setattr(threading, "Timer", _NoRearmTimer)
    monkeypatch.setattr(os, "kill", lambda pid, sig: sent.append((pid, sig)))

    def _fake_exit(code):
        sent.append((os.getpid(), "os._exit"))
        raise SystemExit(code)

    monkeypatch.setattr(os, "_exit", _fake_exit)
    with agent_mode._lock:
        agent_mode._active_count = 0
        agent_mode._active_connection_ids.clear()
        agent_mode._shutdown_attempts = 0
    yield sent
    with agent_mode._lock:
        agent_mode._active_count = 0
        agent_mode._active_connection_ids.clear()
        agent_mode._shutdown_attempts = 0


def test_first_empty_tick_sends_sigterm(watchdog_signals):
    agent_mode._watchdog()

    assert watchdog_signals == [(os.getpid(), signal.SIGTERM)]


def test_grace_ticks_repeat_sigterm_without_escalating(watchdog_signals):
    for _ in range(agent_mode._SHUTDOWN_GRACE_TICKS):
        agent_mode._watchdog()

    assert len(watchdog_signals) == agent_mode._SHUTDOWN_GRACE_TICKS
    assert {sig for _, sig in watchdog_signals} == {signal.SIGTERM}


def test_hung_shutdown_escalates_past_grace(watchdog_signals):
    for _ in range(agent_mode._SHUTDOWN_GRACE_TICKS + 1):
        try:
            agent_mode._watchdog()
        except SystemExit:
            pass

    assert len(watchdog_signals) == agent_mode._SHUTDOWN_GRACE_TICKS + 1
    if hasattr(signal, "SIGKILL"):
        assert watchdog_signals[-1] == (os.getpid(), signal.SIGKILL)
    else:
        assert watchdog_signals[-1] == (os.getpid(), "os._exit")


def test_activity_resets_the_escalation(watchdog_signals):
    agent_mode._watchdog()
    agent_mode._watchdog()

    with agent_mode._lock:
        agent_mode._active_count = 1
    agent_mode._watchdog()
    with agent_mode._lock:
        agent_mode._active_count = 0
    agent_mode._watchdog()

    assert watchdog_signals == [(os.getpid(), signal.SIGTERM)] * 3
