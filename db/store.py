"""SQLAlchemy 2.0 + asyncpg implementation of the LedgerStore Protocol.

Not wired into api.py yet — api.py still defaults to InMemory/Sqlite. Swap in
by constructing a PostgresLedgerStore and handing it to the HTTP layer once
we're ready to connect a real DB.
"""
from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.models import LedgerEntry

_KINDS = ("payment", "debt", "equity", "sub")


class PostgresLedgerStore:
    """LedgerStore backed by Postgres via SQLAlchemy async.

    The store is sync on the outside (to match the Protocol used by api.py's
    stdlib HTTP handler) and async on the inside. Each call spins up its own
    event loop call via asyncio.run; if this moves to an async web framework
    later, expose the `_*_async` methods directly and drop the wrappers.
    """

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sm = sessionmaker

    # ---- sync Protocol surface ----

    def put(self, kind: str, entry_id: str, value: dict[str, Any]) -> None:
        self._check_kind(kind)
        asyncio.run(self._put_async(kind, entry_id, value))

    def get(self, kind: str, entry_id: str) -> dict[str, Any] | None:
        self._check_kind(kind)
        return asyncio.run(self._get_async(kind, entry_id))

    def delete(self, kind: str, entry_id: str) -> bool:
        self._check_kind(kind)
        return asyncio.run(self._delete_async(kind, entry_id))

    def all(self) -> dict[str, dict[str, dict[str, Any]]]:
        return asyncio.run(self._all_async())

    # ---- async internals ----

    async def _put_async(
        self, kind: str, entry_id: str, value: dict[str, Any]
    ) -> None:
        stmt = (
            insert(LedgerEntry)
            .values(kind=kind, entry_id=entry_id, data=value)
            .on_conflict_do_update(
                index_elements=[LedgerEntry.kind, LedgerEntry.entry_id],
                set_={"data": value},
            )
        )
        async with self._sm() as session:
            await session.execute(stmt)
            await session.commit()

    async def _get_async(
        self, kind: str, entry_id: str
    ) -> dict[str, Any] | None:
        stmt = select(LedgerEntry.data).where(
            LedgerEntry.kind == kind, LedgerEntry.entry_id == entry_id
        )
        async with self._sm() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def _delete_async(self, kind: str, entry_id: str) -> bool:
        stmt = delete(LedgerEntry).where(
            LedgerEntry.kind == kind, LedgerEntry.entry_id == entry_id
        )
        async with self._sm() as session:
            result = await session.execute(stmt)
            await session.commit()
            return (result.rowcount or 0) > 0

    async def _all_async(self) -> dict[str, dict[str, dict[str, Any]]]:
        stmt = select(LedgerEntry.kind, LedgerEntry.entry_id, LedgerEntry.data)
        out: dict[str, dict[str, dict[str, Any]]] = {k: {} for k in _KINDS}
        async with self._sm() as session:
            result = await session.execute(stmt)
            for kind, entry_id, data in result.all():
                out.setdefault(kind, {})[entry_id] = data
        return out

    @staticmethod
    def _check_kind(kind: str) -> None:
        if kind not in _KINDS:
            raise ValueError(f"unknown ledger kind: {kind!r}")
