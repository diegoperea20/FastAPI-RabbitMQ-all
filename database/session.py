import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from .base import Base
from config import settings


def _resolve_db_url(url: str) -> str:
    if "sqlite" in url and "+aiosqlite" not in url:
        url = url.replace("sqlite://", "sqlite+aiosqlite://")
    if url.startswith("sqlite+aiosqlite:///./"):
        _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        rel = url[len("sqlite+aiosqlite:///./"):]
        return f"sqlite+aiosqlite:///{os.path.join(_BASE, rel)}"
    return url


_db_url = _resolve_db_url(settings.DATABASE_URL)
engine = create_async_engine(
    _db_url,
    connect_args={"check_same_thread": False} if "sqlite" in _db_url else {},
    echo=settings.DEBUG,
)

AsyncSessionLocal = async_sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    from database.models import Task
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db() -> None:
    await engine.dispose()