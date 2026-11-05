"""
Plaid API client (stdlib-only)
==============================

Thin wrapper around the Plaid REST API. Mirrors the zero-dep posture of
:mod:`client` — calls Plaid via ``urllib.request`` so theta doesn't pull
the ``plaid`` pip dependency into its ``python:3-slim`` image. Plaid's
API is plain JSON-over-HTTPS, so the SDK buys very little versus a
purpose-built client that we can also exception-handle the same way we
handle our own API.

Best-practice posture
---------------------
- ``PLAID_SECRET`` is set server-side only and never travels to the
  browser; the frontend only ever sees the short-lived ``link_token``
  (from /plaid/link-token) and posts back the short-lived
  ``public_token`` (to /plaid/exchange). The long-lived ``access_token``
  is exchanged + persisted server-side and never returned.
- Errors from Plaid carry ``error_type`` / ``error_code`` /
  ``error_message`` in their body — we extract and surface those rather
  than just the HTTP status so callers can react meaningfully.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from typing import Any, Iterable


# ---------- exception hierarchy ----------

class PlaidError(Exception):
    """Base for every Plaid-related failure raised by this module."""


class PlaidConnectionError(PlaidError):
    """DNS / TCP / TLS failure — the request never reached Plaid."""


class PlaidTimeoutError(PlaidError):
    """Socket-level timeout reading from Plaid."""


class PlaidAPIError(PlaidError):
    """Plaid returned a non-2xx response. ``error_type`` / ``error_code``
    / ``error_message`` are pulled from the body when present so callers
    can branch on the specific failure (e.g. ITEM_LOGIN_REQUIRED)."""

    def __init__(
        self,
        status: int,
        error_type: str | None,
        error_code: str | None,
        error_message: str | None,
        request_id: str | None,
        raw_body: str,
    ) -> None:
        bits = [f"HTTP {status}"]
        if error_code:
            bits.append(error_code)
        if error_message:
            bits.append(error_message)
        super().__init__(" — ".join(bits))
        self.status = status
        self.error_type = error_type
        self.error_code = error_code
        self.error_message = error_message
        self.request_id = request_id
        self.raw_body = raw_body


# ---------- client ----------

ENV_HOSTS = {
    "sandbox": "https://sandbox.plaid.com",
    "production": "https://production.plaid.com",
    # Plaid retired the dedicated "development" environment in 2024;
    # callers requesting it land on sandbox so we don't crash startup.
    "development": "https://sandbox.plaid.com",
}


class PlaidClient:
    """Tiny urllib-based client. One method per Plaid endpoint we need.

    The constructor does no I/O — it just stashes credentials. So an
    instance is cheap and safe to hold for the lifetime of the server."""

    def __init__(
        self,
        client_id: str,
        secret: str,
        env: str = "sandbox",
        *,
        timeout: float = 10.0,
        client_name: str = "theta",
    ) -> None:
        if env not in ENV_HOSTS:
            raise ValueError(
                f"unknown PLAID_ENV {env!r}; "
                f"expected one of {sorted(ENV_HOSTS)}"
            )
        self.client_id = client_id
        self.secret = secret
        self.env = env
        self.base = ENV_HOSTS[env]
        self.timeout = timeout
        self.client_name = client_name

    # --- transport ---

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base}{path}"
        payload = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload, method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError as e:
                    raise PlaidAPIError(
                        status=resp.status,
                        error_type=None,
                        error_code="INVALID_JSON_RESPONSE",
                        error_message=f"could not parse Plaid response: {e}",
                        request_id=resp.headers.get("Plaid-Request-Id"),
                        raw_body=raw,
                    ) from e

        except urllib.error.HTTPError as e:
            raw = (e.read() or b"").decode("utf-8", errors="replace")
            err_type = err_code = err_msg = req_id = None
            try:
                parsed = json.loads(raw)
                err_type = parsed.get("error_type")
                err_code = parsed.get("error_code")
                err_msg = parsed.get("error_message") or parsed.get("display_message")
                req_id = parsed.get("request_id")
            except json.JSONDecodeError:
                pass
            raise PlaidAPIError(
                status=e.code,
                error_type=err_type,
                error_code=err_code,
                error_message=err_msg,
                request_id=req_id,
                raw_body=raw,
            ) from e

        except socket.timeout as e:
            raise PlaidTimeoutError(
                f"timeout calling Plaid {path}: {e}"
            ) from e
        except urllib.error.URLError as e:
            raise PlaidConnectionError(
                f"connection failed to Plaid {path}: {e.reason}"
            ) from e

    # --- API methods ---
    #
    # Only the endpoints theta actually uses. Adding a method is cheap
    # (a few lines), so the surface stays minimal until we need more.

    def link_token_create(
        self,
        *,
        user_id: str,
        products: Iterable[str] = ("liabilities",),
        country_codes: Iterable[str] = ("US",),
        language: str = "en",
        webhook: str | None = None,
        redirect_uri: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "user": {"client_user_id": user_id},
            "client_name": self.client_name,
            "products": list(products),
            "country_codes": list(country_codes),
            "language": language,
        }
        if webhook:
            body["webhook"] = webhook
        if redirect_uri:
            # OAuth banks (Chase, Capital One, Amex) bounce back here.
            body["redirect_uri"] = redirect_uri
        return self._post("/link/token/create", body)

    def item_public_token_exchange(self, public_token: str) -> dict[str, Any]:
        return self._post("/item/public_token/exchange", {
            "client_id": self.client_id,
            "secret": self.secret,
            "public_token": public_token,
        })

    def item_get(self, access_token: str) -> dict[str, Any]:
        return self._post("/item/get", {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
        })

    def institutions_get_by_id(
        self,
        institution_id: str,
        *,
        country_codes: Iterable[str] = ("US",),
    ) -> dict[str, Any]:
        return self._post("/institutions/get_by_id", {
            "client_id": self.client_id,
            "secret": self.secret,
            "institution_id": institution_id,
            "country_codes": list(country_codes),
        })

    def liabilities_get(
        self,
        access_token: str,
        *,
        account_ids: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
        }
        if account_ids is not None:
            body["options"] = {"account_ids": list(account_ids)}
        return self._post("/liabilities/get", body)

    def item_remove(self, access_token: str) -> dict[str, Any]:
        return self._post("/item/remove", {
            "client_id": self.client_id,
            "secret": self.secret,
            "access_token": access_token,
        })
