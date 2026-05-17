"""
Ledger API REST client
======================

Stdlib-only client for the theta Ledger API (see ``openapi_spec.py``). No
``requests`` / ``httpx`` dependency — mirrors the zero-dep posture of the
server so consumers running inside the same ``python:3-slim`` image don't
pull a wheel just to talk to it.

Exception hierarchy
-------------------
All errors inherit from :class:`LedgerClientError` so callers can catch
the entire client surface with a single ``except``::

    LedgerClientError
      ├── LedgerHTTPError       — server returned non-2xx (carries .status, .api_error)
      ├── LedgerTimeoutError    — read/connect timed out
      ├── LedgerConnectionError — DNS / refused / network blip
      └── LedgerParseError      — response wasn't JSON or didn't match expected shape

For HTTP errors, the server's ``{"error": "..."}`` message is parsed off
the response body and exposed as :attr:`LedgerHTTPError.api_error` so the
caller doesn't have to crack open a body that's already been read.

Run directly to exercise the client::

    python3 theta/client.py --base-url http://127.0.0.1:8765
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


# ---------- exceptions ----------


class LedgerClientError(Exception):
    """Base exception for every error this client raises.

    Catching this is sufficient to contain *any* failure originating in
    the client — network, HTTP, parsing. Specific subclasses below carry
    structured detail when callers want to discriminate.
    """


class LedgerHTTPError(LedgerClientError):
    """Server returned a non-2xx status.

    ``api_error`` is the string pulled from the response body's
    ``{"error": "..."}`` envelope when present, else ``None``. ``body``
    is the raw response text (truncated) for diagnostics.
    """

    def __init__(self, status: int, url: str, api_error: str | None,
                 body: str) -> None:
        self.status = status
        self.url = url
        self.api_error = api_error
        self.body = body
        msg = f"HTTP {status} on {url}"
        if api_error:
            msg += f": {api_error}"
        super().__init__(msg)


class LedgerTimeoutError(LedgerClientError):
    """A request exceeded the configured timeout."""

    def __init__(self, url: str, timeout: float) -> None:
        self.url = url
        self.timeout = timeout
        super().__init__(f"timeout after {timeout}s on {url}")


class LedgerConnectionError(LedgerClientError):
    """Connection-level failure (DNS, refused, reset, unreachable).

    Distinguished from :class:`LedgerHTTPError` because the iterator
    treats this as DISCONNECTED rather than POLLING_FAILURE — different
    operational signal.
    """

    def __init__(self, url: str, reason: str) -> None:
        self.url = url
        self.reason = reason
        super().__init__(f"connection failed for {url}: {reason}")


class LedgerParseError(LedgerClientError):
    """Response was received but couldn't be turned into the expected shape.

    The ``reason`` field always explains *why* parsing failed (bad JSON,
    missing key, wrong type), and ``raw`` keeps the original payload for
    troubleshooting.
    """

    def __init__(self, url: str, reason: str, raw: Any) -> None:
        self.url = url
        self.reason = reason
        self.raw = raw
        super().__init__(f"parse error for {url}: {reason}")


# ---------- response container ----------


@dataclass
class LedgerEntryRecord:
    """A single ledger entry as returned by the API.

    The server-side schema is ``{"journal": [...], <kind-specific fields>}``
    — we keep ``data`` as the raw dict and let callers index into it,
    since the kind-specific shape isn't worth typing exhaustively.
    """

    kind: str
    entry_id: str
    data: dict[str, Any]


# ---------- client ----------


_DEFAULT_TIMEOUT = 10.0
_KINDS = ("payment", "debt", "equity", "sub")


class LedgerClient:
    """Synchronous client for the Ledger API.

    The iterator wraps blocking calls in ``asyncio.to_thread`` rather
    than carrying a second async-aware HTTP stack, so this stays sync.
    """

    def __init__(self, base_url: str, *, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ----- public surface -----

    def health(self) -> dict[str, Any]:
        return self._get_json("/healthz")

    def list_all(self) -> dict[str, dict[str, dict[str, Any]]]:
        """``GET /ledger`` — all entries grouped by kind, then id."""
        data = self._get_json("/ledger")
        if not isinstance(data, dict):
            raise LedgerParseError(self._url("/ledger"),
                                   "expected object at top level", data)
        return data

    def get(self, kind: str, entry_id: str) -> LedgerEntryRecord:
        self._check_kind(kind)
        path = f"/ledger/{kind}/{urllib.parse.quote(entry_id, safe='')}"
        data = self._get_json(path)
        if not isinstance(data, dict):
            raise LedgerParseError(self._url(path),
                                   "expected object", data)
        return LedgerEntryRecord(kind=kind, entry_id=entry_id, data=data)

    def put(self, kind: str, entry_id: str,
            body: dict[str, Any]) -> LedgerEntryRecord:
        self._check_kind(kind)
        path = f"/ledger/{kind}/{urllib.parse.quote(entry_id, safe='')}"
        data = self._request_json("PUT", path, body=body)
        if not isinstance(data, dict):
            raise LedgerParseError(self._url(path),
                                   "PUT response was not an object", data)
        return LedgerEntryRecord(kind=kind, entry_id=entry_id, data=data)

    def delete(self, kind: str, entry_id: str) -> bool:
        """Returns True iff an entry was actually removed (server reports
        ``deleted: true``); False on 404. Other statuses raise."""
        self._check_kind(kind)
        path = f"/ledger/{kind}/{urllib.parse.quote(entry_id, safe='')}"
        try:
            data = self._request_json("DELETE", path)
        except LedgerHTTPError as e:
            if e.status == 404:
                return False
            raise
        if not isinstance(data, dict) or "deleted" not in data:
            raise LedgerParseError(self._url(path),
                                   "DELETE response missing 'deleted'", data)
        return bool(data["deleted"])

    # ----- internals -----

    @staticmethod
    def _check_kind(kind: str) -> None:
        if kind not in _KINDS:
            raise ValueError(f"unknown kind {kind!r}; expected one of {_KINDS}")

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _get_json(self, path: str) -> Any:
        return self._request_json("GET", path)

    def _request_json(self, method: str, path: str,
                      body: dict[str, Any] | None = None) -> Any:
        url = self._url(path)
        data_bytes = json.dumps(body, default=str).encode() if body is not None else None
        headers = {"Accept": "application/json"}
        if data_bytes is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data_bytes, method=method,
                                     headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            # 4xx/5xx — server responded, body may carry an {"error": ...}.
            body_text = self._safe_read(e)
            api_error = self._extract_api_error(body_text)
            raise LedgerHTTPError(e.code, url, api_error, body_text) from e
        except urllib.error.URLError as e:
            # Connection-layer failure: DNS, refused, no route, TLS, etc.
            # urllib raises URLError(socket.timeout(...)) for read timeouts
            # on the urlopen call too — surface as a timeout.
            if isinstance(e.reason, (socket.timeout, TimeoutError)):
                raise LedgerTimeoutError(url, self.timeout) from e
            raise LedgerConnectionError(url, str(e.reason)) from e
        except (socket.timeout, TimeoutError) as e:
            raise LedgerTimeoutError(url, self.timeout) from e

        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise LedgerParseError(
                url,
                f"response was not valid JSON: {e.msg} (line {e.lineno})",
                raw[:500].decode(errors="replace"),
            ) from e

    @staticmethod
    def _safe_read(err: urllib.error.HTTPError) -> str:
        try:
            return err.read().decode(errors="replace")
        except Exception:
            return ""

    @staticmethod
    def _extract_api_error(body_text: str) -> str | None:
        if not body_text:
            return None
        try:
            payload = json.loads(body_text)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            err = payload.get("error")
            if isinstance(err, str):
                return err
        return None


# ---------- embedded demo ----------


def _demo(base_url: str, timeout: float) -> int:
    client = LedgerClient(base_url, timeout=timeout)

    print(f"# health  GET {base_url}/healthz")
    try:
        print(json.dumps(client.health()))
    except LedgerClientError as e:
        # Health is the canary — if it fails we can't continue.
        print(f"health failed ({type(e).__name__}): {e}", file=sys.stderr)
        return 1

    demo_id = "client-demo-1"
    print(f"\n# put    PUT  /ledger/equity/{demo_id}")
    try:
        rec = client.put("equity", demo_id, {
            "source": "demo",
            "amount": "100.00",
            "received_on": "2026-05-09",
            "memo": "client.py self-test",
        })
        print(json.dumps(rec.data, default=str)[:200] + " ...")
    except LedgerHTTPError as e:
        print(f"PUT rejected by server: status={e.status} api_error={e.api_error}",
              file=sys.stderr)
    except LedgerClientError as e:
        print(f"PUT failed ({type(e).__name__}): {e}", file=sys.stderr)

    print("\n# list   GET  /ledger")
    try:
        all_entries = client.list_all()
        for kind, entries in all_entries.items():
            print(f"  {kind}: {len(entries)} entries")
    except LedgerClientError as e:
        print(f"list failed ({type(e).__name__}): {e}", file=sys.stderr)

    print(f"\n# get    GET  /ledger/equity/{demo_id}")
    try:
        rec = client.get("equity", demo_id)
        print(f"  ok — kind={rec.kind} id={rec.entry_id} keys={list(rec.data)}")
    except LedgerHTTPError as e:
        print(f"GET rejected: {e.status} {e.api_error}", file=sys.stderr)

    print(f"\n# delete DELETE /ledger/equity/{demo_id}")
    try:
        existed = client.delete("equity", demo_id)
        print(f"  deleted={existed}")
    except LedgerClientError as e:
        print(f"delete failed ({type(e).__name__}): {e}", file=sys.stderr)

    # Demonstrate that a parse-error path raises a typed exception. We
    # simulate this by hitting an endpoint we know returns HTML.
    print("\n# parse  GET  / (HTML, not JSON — must raise LedgerParseError)")
    try:
        client._get_json("/")
    except LedgerParseError as e:
        print(f"  raised LedgerParseError as expected: {e.reason}")
    except LedgerClientError as e:
        print(f"  raised {type(e).__name__}: {e}")

    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ledger API REST client demo")
    p.add_argument("--base-url",
                   default=os.environ.get("THETA_URL", "http://127.0.0.1:8765"),
                   help="Theta base URL (env: THETA_URL)")
    p.add_argument("--timeout", type=float,
                   default=float(os.environ.get("THETA_TIMEOUT", _DEFAULT_TIMEOUT)),
                   help="HTTP timeout in seconds (env: THETA_TIMEOUT)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    return _demo(args.base_url, args.timeout)


if __name__ == "__main__":
    sys.exit(main())
