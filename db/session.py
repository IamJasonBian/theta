import os

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def get_engine(url: str | None = None) -> AsyncEngine:
    return create_async_engine(url or _database_url(), pool_pre_ping=True)


def get_sessionmaker(
    engine: AsyncEngine | None = None,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine or get_engine(),
        expire_on_commit=False,
        class_=AsyncSession,
    )
