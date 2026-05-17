"""
Async polling iterator over the Ledger API
==========================================

A never-ending ``async for`` source of :class:`LedgerEvent` objects driven
by polling ``GET /ledger`` on a fixed interval. Each successful poll is
diffed against the previous snapshot to produce ``new`` / ``updated`` /
``deleted`` change records bundled into an :class:`EventBatch`. Network
and HTTP failures surface as DISCONNECTED / POLLING_FAILURE events rather
than exceptions — the iterator never stops on its own; only ``stop()``
or :class:`StopAsyncIteration` ends it.

States
------
- ``STARTED``         — first event yielded after construction
- ``BATCH``           — a successful poll; carries an :class:`EventBatch`
- ``DISCONNECTED``    — connection-layer failure (DNS, refused, timeout)
- ``POLLING_FAILURE`` — server responded with non-2xx, or response failed parsing
- ``STOPPED``         — yielded once before :class:`StopAsyncIteration`

Stop & cleanup
--------------
``stop()`` flips an asyncio event the consumer task watches. The poller
finishes any in-flight HTTP call, emits ``STOPPED``, then raises
:class:`StopAsyncIteration`. ``aclose()`` is provided for ``async with``
parity. Calling ``stop()`` twice is a no-op.

Run directly to exercise the iterator against a running theta server::

    python3 theta/iterator.py --base-url http://127.0.0.1:8765 --interval 2
"""

from __future__ import annotations

import argparse
import asyncio
import enum
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any

# Allow `python theta/iterator.py` (run from repo root) to find client.py
# sitting next to this file. Mirrors what scripts/fetch_rh_equity.py does.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from client import (  # noqa: E402
    LedgerClient,
    LedgerClientError,
    LedgerConnectionError,
    LedgerHTTPError,
    LedgerParseError,
    LedgerTimeoutError,
)


# ---------- event model ----------


class EventState(str, enum.Enum):
    STARTED = "started"
    BATCH = "batch"
    DISCONNECTED = "disconnected"
    POLLING_FAILURE = "polling_failure"
    STOPPED = "stopped"


@dataclass
class EntryChange:
    """A single change observed between two polls.

    ``change`` is one of ``new`` / ``updated`` / ``deleted``. For
    ``deleted``, ``data`` is the *previous* snapshot — the server no
    longer has it, so this is the only place the consumer can see what
    just disappeared.
    """

    kind: str
    entry_id: str
    change: str
    data: dict[str, Any]


@dataclass
class ParsingFailure:
    """A single record we couldn't parse out of the snapshot.

    Kept on the batch alongside successfully parsed entries so consumers
    don't have to choose between "all-or-nothing" semantics and silent
    drops. ``reason`` always explains *why*.
    """

    kind: str | None
    entry_id: str | None
    reason: str
    raw: Any


@dataclass
class EventBatch:
    """Container holding both successfully parsed changes and per-entry
    parsing failures from a single poll. Either list may be empty; an
    empty batch (no diff vs. last poll, no failures) is still emitted so
    consumers can use it as a heartbeat."""

    parsed: list[EntryChange] = field(default_factory=list)
    failures: list[ParsingFailure] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.parsed and not self.failures


@dataclass
class LedgerEvent:
    """A single event yielded by :class:`LedgerEventIterator`.

    Exactly one of ``batch`` / ``error`` is populated for non-lifecycle
    states; ``STARTED`` and ``STOPPED`` carry neither.
    """

    state: EventState
    batch: EventBatch | None = None
    error: str | None = None
    error_type: str | None = None


# ---------- iterator ----------


_DEFAULT_INTERVAL = 5.0


class LedgerEventIterator:
    """Async iterator that polls the Ledger API and yields change events.

    The diff algorithm is a straight key-by-key comparison of the
    snapshot returned by ``/ledger`` against the previous snapshot. We
    use ``json.dumps(sort_keys=True)`` as a cheap content hash so we
    don't try to deep-equal nested decimals & dates that came in as
    strings anyway.

    Backoff: on consecutive failures, the next-poll delay grows linearly
    up to ``max_backoff`` to avoid hammering a downed server. It resets
    to ``interval`` on the first successful poll.
    """

    def __init__(self,
                 client: LedgerClient,
                 *,
                 interval: float = _DEFAULT_INTERVAL,
                 max_backoff: float = 60.0,
                 emit_initial_snapshot: bool = True) -> None:
        self.client = client
        self.interval = interval
        self.max_backoff = max_backoff
        # When True, the very first poll's contents are emitted as "new"
        # entries; when False, the first poll seeds the baseline silently
        # and only later changes are reported. The handler defaults to
        # True so a fresh consumer sees what already exists.
        self.emit_initial_snapshot = emit_initial_snapshot

        self._stop = asyncio.Event()
        self._started_emitted = False
        self._stopped_emitted = False
        self._consecutive_failures = 0
        # _last is keyed (kind, entry_id) -> content-hash -> raw dict
        self._last: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
        self._seeded = False

    # ----- lifecycle -----

    def __aiter__(self) -> "LedgerEventIterator":
        return self

    async def __anext__(self) -> LedgerEvent:
        if not self._started_emitted:
            self._started_emitted = True
            return LedgerEvent(state=EventState.STARTED)

        if self._stop.is_set():
            return self._finalize()

        await self._sleep_or_stop(self._next_delay())
        if self._stop.is_set():
            return self._finalize()

        return await self._poll_once()

    async def stop(self) -> None:
        """Signal the iterator to wind down. Safe to call from any task,
        including from inside an event consumer. Idempotent."""
        self._stop.set()

    async def aclose(self) -> None:
        """Async-context-manager parity. Calls :meth:`stop`."""
        await self.stop()

    # ----- internals -----

    def _finalize(self) -> LedgerEvent:
        if not self._stopped_emitted:
            self._stopped_emitted = True
            return LedgerEvent(state=EventState.STOPPED)
        raise StopAsyncIteration

    def _next_delay(self) -> float:
        if self._consecutive_failures == 0:
            return self.interval
        # Linear backoff: interval * (1 + failures), capped. Linear (vs.
        # exponential) keeps recovery quick once the server is back —
        # we'd rather over-poll for a moment than miss minutes of events.
        return min(self.interval * (1 + self._consecutive_failures),
                   self.max_backoff)

    async def _sleep_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            return

    async def _poll_once(self) -> LedgerEvent:
        try:
            snapshot = await asyncio.to_thread(self.client.list_all)
        except LedgerTimeoutError as e:
            return self._fail_event(EventState.DISCONNECTED, e)
        except LedgerConnectionError as e:
            return self._fail_event(EventState.DISCONNECTED, e)
        except LedgerHTTPError as e:
            return self._fail_event(EventState.POLLING_FAILURE, e)
        except LedgerParseError as e:
            return self._fail_event(EventState.POLLING_FAILURE, e)
        except LedgerClientError as e:
            # Defensive: any future client subclass we don't know about.
            return self._fail_event(EventState.POLLING_FAILURE, e)

        self._consecutive_failures = 0
        batch = self._diff(snapshot)
        return LedgerEvent(state=EventState.BATCH, batch=batch)

    def _fail_event(self, state: EventState,
                    err: LedgerClientError) -> LedgerEvent:
        self._consecutive_failures += 1
        return LedgerEvent(
            state=state,
            error=str(err),
            error_type=type(err).__name__,
        )

    def _diff(self, snapshot: dict[str, Any]) -> EventBatch:
        batch = EventBatch()
        seen: set[tuple[str, str]] = set()

        if not isinstance(snapshot, dict):
            batch.failures.append(ParsingFailure(
                kind=None, entry_id=None,
                reason=f"top-level snapshot was {type(snapshot).__name__}, "
                       "expected object",
                raw=snapshot,
            ))
            return batch

        for kind, entries in snapshot.items():
            if not isinstance(entries, dict):
                batch.failures.append(ParsingFailure(
                    kind=kind, entry_id=None,
                    reason=f"kind bucket was {type(entries).__name__}, "
                           "expected object",
                    raw=entries,
                ))
                continue
            for entry_id, data in entries.items():
                if not isinstance(data, dict):
                    batch.failures.append(ParsingFailure(
                        kind=kind, entry_id=entry_id,
                        reason=f"entry was {type(data).__name__}, "
                               "expected object",
                        raw=data,
                    ))
                    continue
                key = (kind, entry_id)
                seen.add(key)
                try:
                    h = json.dumps(data, sort_keys=True, default=str)
                except (TypeError, ValueError) as e:
                    batch.failures.append(ParsingFailure(
                        kind=kind, entry_id=entry_id,
                        reason=f"could not serialize for diff: {e}",
                        raw=data,
                    ))
                    continue
                prev = self._last.get(key)
                if prev is None:
                    if self._seeded or self.emit_initial_snapshot:
                        batch.parsed.append(EntryChange(
                            kind=kind, entry_id=entry_id,
                            change="new", data=data,
                        ))
                elif prev[0] != h:
                    batch.parsed.append(EntryChange(
                        kind=kind, entry_id=entry_id,
                        change="updated", data=data,
                    ))
                self._last[key] = (h, data)

        for key in list(self._last.keys()):
            if key not in seen:
                kind, entry_id = key
                _, prev_data = self._last.pop(key)
                batch.parsed.append(EntryChange(
                    kind=kind, entry_id=entry_id,
                    change="deleted", data=prev_data,
                ))

        self._seeded = True
        return batch


# ---------- embedded demo ----------


async def _demo(base_url: str, interval: float, timeout: float,
                duration: float | None) -> int:
    client = LedgerClient(base_url, timeout=timeout)
    it = LedgerEventIterator(client, interval=interval)

    loop = asyncio.get_running_loop()
    stop_handle = asyncio.Event()

    def _request_stop() -> None:
        stop_handle.set()

    # Ctrl-C / SIGTERM → graceful stop. Wrapped in try because Windows
    # event loops don't support add_signal_handler — but theta runs in
    # a Linux container, so this is the common path.
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass

    if duration is not None:
        loop.call_later(duration, _request_stop)

    async def watch_stop() -> None:
        await stop_handle.wait()
        await it.stop()

    watcher = asyncio.create_task(watch_stop())

    try:
        async for event in it:
            if event.state is EventState.STARTED:
                print(f"[start] polling {base_url} every {interval}s")
                continue
            if event.state is EventState.STOPPED:
                print("[stop] iterator drained, exiting")
                break
            if event.state is EventState.BATCH:
                assert event.batch is not None
                if event.batch.is_empty():
                    print("[batch] heartbeat (no changes)")
                else:
                    for ch in event.batch.parsed:
                        print(f"[batch] {ch.change:8s} {ch.kind}/{ch.entry_id}")
                    for f in event.batch.failures:
                        print(f"[batch] PARSE-FAIL {f.kind}/{f.entry_id}: {f.reason}")
                continue
            # disconnected / polling_failure
            print(f"[{event.state.value}] {event.error_type}: {event.error}")
    finally:
        await it.stop()
        watcher.cancel()
        try:
            await watcher
        except asyncio.CancelledError:
            pass

    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Async ledger event iterator demo")
    p.add_argument("--base-url",
                   default=os.environ.get("THETA_URL", "http://127.0.0.1:8765"))
    p.add_argument("--interval", type=float,
                   default=float(os.environ.get("THETA_POLL_INTERVAL",
                                                _DEFAULT_INTERVAL)),
                   help="seconds between polls (env: THETA_POLL_INTERVAL)")
    p.add_argument("--timeout", type=float,
                   default=float(os.environ.get("THETA_TIMEOUT", "10")),
                   help="HTTP timeout (env: THETA_TIMEOUT)")
    p.add_argument("--duration", type=float, default=None,
                   help="auto-stop after N seconds (handy for CI smoke tests)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return asyncio.run(_demo(args.base_url, args.interval,
                             args.timeout, args.duration))


if __name__ == "__main__":
    sys.exit(main())
