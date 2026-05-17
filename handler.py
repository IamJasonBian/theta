"""
Ledger event handler
====================

Glue layer that wires :class:`LedgerClient` and :class:`LedgerEventIterator`
together, drives a long-running consume loop, maintains rolling state
based on what's been observed, and fires user-supplied callbacks when
something the operator cares about happens.

The handler is intentionally callback-driven rather than yielding events
itself — the iterator already exposes that surface; this layer is for
"do X when Y" plumbing (alerts, downstream writes, dashboards). Callers
that just want raw events can keep using the iterator directly.

State tracked
-------------
- per-kind counts: how many entries currently in the ledger
- per-change counts: total new / updated / deleted observed since start
- ``consecutive_failures``: resets on a successful poll
- ``last_batch_at``: monotonic timestamp of last successful poll
- ``parse_failures``: aggregate count for monitoring

Callbacks
---------
All callbacks are optional and may be sync or async. If a sync callback
raises, the handler logs and continues — a misbehaving callback should
not take down the consume loop. Async callbacks are awaited inline so
back-pressure naturally throttles polling.

Run directly::

    python3 theta/handler.py --base-url http://127.0.0.1:8765 --interval 5
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from client import LedgerClient  # noqa: E402
from iterator import (  # noqa: E402
    EntryChange,
    EventBatch,
    EventState,
    LedgerEvent,
    LedgerEventIterator,
    ParsingFailure,
)


log = logging.getLogger("theta.handler")


# ---------- callback types ----------

# Each callback can be sync or async. We unify with `_maybe_await`.
_Sync = Callable[..., Any]
_Async = Callable[..., Awaitable[Any]]
Callback = _Sync | _Async


@dataclass
class HandlerCallbacks:
    """Bundle of optional callbacks the handler fires.

    Use ``None`` for any you don't care about. Signatures:

    - ``on_new(change: EntryChange)``
    - ``on_update(change: EntryChange)``
    - ``on_delete(change: EntryChange)``
    - ``on_parse_failure(failure: ParsingFailure)``
    - ``on_disconnected(error_type: str, error: str)``
    - ``on_polling_failure(error_type: str, error: str)``
    - ``on_alert(reason: str, state: HandlerState)`` — fires when
      ``consecutive_failures`` crosses ``alert_after_failures``
    - ``on_started()`` / ``on_stopped()`` — lifecycle hooks
    """

    on_new: Callback | None = None
    on_update: Callback | None = None
    on_delete: Callback | None = None
    on_parse_failure: Callback | None = None
    on_disconnected: Callback | None = None
    on_polling_failure: Callback | None = None
    on_alert: Callback | None = None
    on_started: Callback | None = None
    on_stopped: Callback | None = None


@dataclass
class HandlerState:
    """Rolling state, updated as events flow in."""

    by_kind: dict[str, int] = field(default_factory=dict)
    new_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    parse_failure_count: int = 0
    consecutive_failures: int = 0
    total_failures: int = 0
    last_batch_at: float | None = None
    last_event_state: str | None = None
    started_at: float | None = None
    alert_active: bool = False


# ---------- handler ----------


class LedgerEventHandler:
    """Consume events from a :class:`LedgerEventIterator` and fire callbacks.

    Construction
    ------------
    ``handler = LedgerEventHandler(client, iterator, callbacks=...)`` then
    ``await handler.run()``. The factory :meth:`from_args` builds the
    full client+iterator+handler stack from an :class:`argparse.Namespace`.

    Stop semantics
    --------------
    ``stop()`` cascades to the iterator. ``run()`` returns once the
    iterator has emitted ``STOPPED``.
    """

    def __init__(self,
                 client: LedgerClient,
                 iterator: LedgerEventIterator,
                 *,
                 callbacks: HandlerCallbacks | None = None,
                 alert_after_failures: int = 3) -> None:
        self.client = client
        self.iterator = iterator
        self.callbacks = callbacks or HandlerCallbacks()
        self.alert_after_failures = alert_after_failures
        self.state = HandlerState()
        self._stopping = False

    # ----- public surface -----

    @classmethod
    def from_args(cls, args: argparse.Namespace,
                  callbacks: HandlerCallbacks | None = None
                  ) -> "LedgerEventHandler":
        """Build the full stack from CLI args. The handler owns the
        client and iterator it constructs — pass an explicit instance if
        you need to share them with other consumers."""
        client = LedgerClient(args.base_url, timeout=args.timeout)
        iterator = LedgerEventIterator(
            client,
            interval=args.interval,
            max_backoff=args.max_backoff,
            emit_initial_snapshot=not args.skip_initial_snapshot,
        )
        return cls(
            client, iterator,
            callbacks=callbacks,
            alert_after_failures=args.alert_after_failures,
        )

    async def run(self) -> HandlerState:
        """Drive the consume loop. Returns the final state when stopped."""
        self.state.started_at = time.monotonic()
        try:
            async for event in self.iterator:
                await self._on_event(event)
                if event.state is EventState.STOPPED:
                    break
        finally:
            # Best-effort cleanup — if run() unwinds via exception, still
            # tell the iterator to drain any in-flight HTTP call.
            await self.iterator.stop()
        return self.state

    async def stop(self) -> None:
        """Request graceful shutdown. Idempotent."""
        if self._stopping:
            return
        self._stopping = True
        await self.iterator.stop()

    # ----- event dispatch -----

    async def _on_event(self, event: LedgerEvent) -> None:
        self.state.last_event_state = event.state.value

        if event.state is EventState.STARTED:
            await self._fire(self.callbacks.on_started)
            return

        if event.state is EventState.STOPPED:
            await self._fire(self.callbacks.on_stopped)
            return

        if event.state is EventState.BATCH:
            assert event.batch is not None
            await self._on_batch(event.batch)
            return

        # disconnected / polling_failure
        self.state.consecutive_failures += 1
        self.state.total_failures += 1
        cb = (self.callbacks.on_disconnected
              if event.state is EventState.DISCONNECTED
              else self.callbacks.on_polling_failure)
        await self._fire(cb, event.error_type, event.error)
        await self._maybe_alert(reason=f"{event.state.value}: {event.error}")

    async def _on_batch(self, batch: EventBatch) -> None:
        self.state.consecutive_failures = 0
        self.state.last_batch_at = time.monotonic()
        if self.state.alert_active:
            self.state.alert_active = False
            log.info("recovered: poll succeeded after failure streak")

        for change in batch.parsed:
            self._update_counts(change)
            cb = self._cb_for_change(change.change)
            await self._fire(cb, change)

        for failure in batch.failures:
            self.state.parse_failure_count += 1
            await self._fire(self.callbacks.on_parse_failure, failure)

    def _cb_for_change(self, change: str) -> Callback | None:
        if change == "new":
            return self.callbacks.on_new
        if change == "updated":
            return self.callbacks.on_update
        if change == "deleted":
            return self.callbacks.on_delete
        return None

    def _update_counts(self, change: EntryChange) -> None:
        if change.change == "new":
            self.state.new_count += 1
            self.state.by_kind[change.kind] = (
                self.state.by_kind.get(change.kind, 0) + 1
            )
        elif change.change == "updated":
            self.state.updated_count += 1
        elif change.change == "deleted":
            self.state.deleted_count += 1
            self.state.by_kind[change.kind] = max(
                0, self.state.by_kind.get(change.kind, 0) - 1
            )

    async def _maybe_alert(self, reason: str) -> None:
        if (self.state.consecutive_failures >= self.alert_after_failures
                and not self.state.alert_active):
            self.state.alert_active = True
            log.warning("alert: %d consecutive failures (%s)",
                        self.state.consecutive_failures, reason)
            await self._fire(self.callbacks.on_alert, reason, self.state)

    async def _fire(self, cb: Callback | None, *args: Any) -> None:
        if cb is None:
            return
        try:
            result = cb(*args)
            if inspect.isawaitable(result):
                await result
        except Exception:  # noqa: BLE001 — callbacks must not break the loop
            log.exception("callback %r failed", getattr(cb, "__name__", cb))


# ---------- CLI ----------


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Register all client/iterator/handler args on ``parser``.

    Exposed as a separate function so other entrypoints (Airflow DAGs,
    Render-side wrappers) can reuse the exact same argument surface.
    """
    # client
    parser.add_argument(
        "--base-url",
        default=os.environ.get("THETA_URL", "http://127.0.0.1:8765"),
        help="Theta base URL (env: THETA_URL)")
    parser.add_argument(
        "--timeout", type=float,
        default=float(os.environ.get("THETA_TIMEOUT", "10")),
        help="HTTP timeout in seconds (env: THETA_TIMEOUT)")

    # iterator
    parser.add_argument(
        "--interval", type=float,
        default=float(os.environ.get("THETA_POLL_INTERVAL", "5")),
        help="seconds between polls (env: THETA_POLL_INTERVAL)")
    parser.add_argument(
        "--max-backoff", type=float,
        default=float(os.environ.get("THETA_MAX_BACKOFF", "60")),
        help="max delay between polls when failing (env: THETA_MAX_BACKOFF)")
    parser.add_argument(
        "--skip-initial-snapshot", action="store_true",
        default=(os.environ.get("THETA_SKIP_INITIAL_SNAPSHOT", "") == "1"),
        help="don't replay existing entries as 'new' on startup "
             "(env: THETA_SKIP_INITIAL_SNAPSHOT=1)")

    # handler
    parser.add_argument(
        "--alert-after-failures", type=int,
        default=int(os.environ.get("THETA_ALERT_AFTER_FAILURES", "3")),
        help="fire on_alert after N consecutive failures "
             "(env: THETA_ALERT_AFTER_FAILURES)")
    parser.add_argument(
        "--duration", type=float, default=None,
        help="auto-stop after N seconds (handy for CI smoke tests)")
    parser.add_argument(
        "--verbose", action="store_true",
        default=(os.environ.get("THETA_HANDLER_VERBOSE", "") == "1"),
        help="enable INFO logging (env: THETA_HANDLER_VERBOSE=1)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ledger event handler")
    add_arguments(p)
    return p.parse_args(argv)


# ---------- embedded demo ----------


def _print_callbacks() -> HandlerCallbacks:
    """Wire up a noisy stdout-printing callback set for the demo. Real
    deployments would replace this with Slack alerts, DB writes, etc."""

    def on_started() -> None:
        print("[started]")

    def on_stopped() -> None:
        print("[stopped]")

    def on_new(c: EntryChange) -> None:
        print(f"[new]      {c.kind}/{c.entry_id}")

    def on_update(c: EntryChange) -> None:
        print(f"[updated]  {c.kind}/{c.entry_id}")

    def on_delete(c: EntryChange) -> None:
        print(f"[deleted]  {c.kind}/{c.entry_id}")

    def on_parse_failure(f: ParsingFailure) -> None:
        print(f"[parse-fail] {f.kind}/{f.entry_id}: {f.reason}")

    def on_disconnected(err_type: str, err: str) -> None:
        print(f"[disconnected] {err_type}: {err}")

    def on_polling_failure(err_type: str, err: str) -> None:
        print(f"[polling-failure] {err_type}: {err}")

    def on_alert(reason: str, state: HandlerState) -> None:
        print(f"[ALERT] {reason} — failures={state.consecutive_failures}")

    return HandlerCallbacks(
        on_started=on_started,
        on_stopped=on_stopped,
        on_new=on_new,
        on_update=on_update,
        on_delete=on_delete,
        on_parse_failure=on_parse_failure,
        on_disconnected=on_disconnected,
        on_polling_failure=on_polling_failure,
        on_alert=on_alert,
    )


async def _run(args: argparse.Namespace) -> int:
    handler = LedgerEventHandler.from_args(args, callbacks=_print_callbacks())
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        loop.create_task(handler.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    if args.duration is not None:
        loop.call_later(args.duration, _request_stop)

    state = await handler.run()
    print("\n--- final state ---")
    print(f"by_kind            : {state.by_kind}")
    print(f"new/upd/del        : {state.new_count}/{state.updated_count}/"
          f"{state.deleted_count}")
    print(f"parse_failures     : {state.parse_failure_count}")
    print(f"total_failures     : {state.total_failures}")
    print(f"alert_active       : {state.alert_active}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
