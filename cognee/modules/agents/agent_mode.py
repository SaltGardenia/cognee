"""Agent mode: automatic server shutdown when no agents are active.

When COGNEE_AGENT_MODE=true, the server tracks how many agent connections
have registered via the /register endpoint. A background watchdog thread
starts on the first registration and checks every 60 seconds whether
any connections remain. If the count drops to zero the watchdog sends
SIGTERM, shutting the server down gracefully. A graceful shutdown that
hangs keeps receiving SIGTERM for a grace period, after which the
watchdog force-kills the process, so the server cannot idle forever.

This is designed for ephemeral deployments where an external
orchestrator spins up a Cognee server for one or more agents. Each
agent calls /register on connect and /unregister when done. Once all
connections finish, the server tears itself down automatically instead of
idling.

The watchdog does NOT start until at least one connection registers, so a
server launched with COGNEE_AGENT_MODE=true will stay alive
indefinitely while waiting for its first connection.
"""

from __future__ import annotations

import os
import signal
import threading
from typing import TYPE_CHECKING

from cognee.shared.logging_utils import get_logger

if TYPE_CHECKING:
    from cognee.modules.agents.models import (
        AgentConnection,
        RegisterAgentRequest,
        UnregisterAgentRequest,
    )
    from cognee.modules.users.models.User import User

logger = get_logger(__name__)

_WATCHDOG_INTERVAL_SECONDS = 60.0
# Empty ticks that may spend on graceful SIGTERM attempts before the watchdog
# force-kills a process whose shutdown is hung (#6018).
_SHUTDOWN_GRACE_TICKS = 3


def set_agent_mode(enabled: bool) -> None:
    os.environ["COGNEE_AGENT_MODE"] = str(enabled).lower()


def is_agent_mode_enabled() -> bool:
    return os.getenv("COGNEE_AGENT_MODE", "false").lower() == "true"


_lock = threading.Lock()
_active_count = 0
_active_connection_ids: set[str] = set()
_watchdog_started = False
# Consecutive empty watchdog ticks that already asked for a graceful shutdown.
# A graceful exit tears the process down between ticks; reaching the grace
# limit with the process still alive means uvicorn's shutdown is hung.
_shutdown_attempts = 0


def _shutdown_server():
    """Ask this process to exit, escalating when graceful shutdown hangs.

    The first tick sends SIGTERM; while the grace limit is not reached it
    warns and sends SIGTERM again, and past it the process is force-killed
    (``SIGKILL``, or ``os._exit`` where there is no SIGKILL), so a hung
    shutdown cannot leave the server idling forever (#6018).
    """
    global _shutdown_attempts

    with _lock:
        _shutdown_attempts += 1
        attempts = _shutdown_attempts

    if attempts == 1:
        logger.info("No active agent connections remaining — shutting down server")
        os.kill(os.getpid(), signal.SIGTERM)
        return
    if attempts <= _SHUTDOWN_GRACE_TICKS:
        logger.warning(
            "Graceful shutdown has not completed after %d SIGTERM attempts; retrying "
            "(force kill after %d ticks)",
            attempts - 1,
            _SHUTDOWN_GRACE_TICKS,
        )
        os.kill(os.getpid(), signal.SIGTERM)
        return
    logger.warning(
        "Graceful shutdown hung for over %d s despite %d SIGTERM attempts — force killing",
        int((_SHUTDOWN_GRACE_TICKS + 1) * _WATCHDOG_INTERVAL_SECONDS),
        attempts - 1,
    )
    if hasattr(signal, "SIGKILL"):
        os.kill(os.getpid(), signal.SIGKILL)
    else:
        os._exit(1)


def _watchdog():
    global _shutdown_attempts

    timer = threading.Timer(_WATCHDOG_INTERVAL_SECONDS, _watchdog)
    timer.daemon = True
    timer.start()

    with _lock:
        count = _active_count
        if count > 0:
            # Activity while a shutdown is pending: the server is wanted again,
            # so the escalation counter starts over.
            _shutdown_attempts = 0

    if count <= 0:
        _shutdown_server()


async def register_agent(user: User, request: RegisterAgentRequest) -> AgentConnection:
    global _active_count, _watchdog_started

    from cognee.modules.agents.operations import register_agent_from_request

    connection = await register_agent_from_request(user, request)

    with _lock:
        if connection.id in _active_connection_ids:
            return connection

        _active_connection_ids.add(connection.id)
        _active_count += 1
        count = _active_count

        if is_agent_mode_enabled() and not _watchdog_started:
            _watchdog_started = True
            timer = threading.Timer(_WATCHDOG_INTERVAL_SECONDS, _watchdog)
            timer.daemon = True
            timer.start()
            logger.info("Agent mode watchdog started")

    logger.info("Agent connection registered (active: %d)", count)
    return connection


async def unregister_agent(user: User, request: UnregisterAgentRequest) -> int:
    """Deactivate the caller's connection; return how many of theirs stay active.

    The process-wide counter below feeds the agent-mode watchdog only. It
    spans every user, so it is never returned: the caller learns about their
    own connections, not how many other users are connected.
    """
    global _active_count

    from cognee.modules.agents.registry import (
        build_agent_connection_id,
        deactivate_agent_connection,
        list_registered_agent_connections,
    )

    connection_id = build_agent_connection_id(
        agent_session_name=request.agent_session_name,
        user_id=str(user.id) if user.id is not None else None,
    )

    await deactivate_agent_connection(user.id, connection_id)

    with _lock:
        if connection_id in _active_connection_ids:
            _active_connection_ids.discard(connection_id)
            _active_count = max(0, _active_count - 1)
        total = _active_count

    callers_count = sum(
        1 for connection in list_registered_agent_connections() if connection.user_id == user.id
    )
    logger.info(
        "Agent connection unregistered (caller active: %d, total active: %d)",
        callers_count,
        total,
    )
    return callers_count


def get_active_count() -> int:
    with _lock:
        return _active_count
