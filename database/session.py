import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .base import Base
from config import settings


def _resolve_db_url(url: str) -> str:
    if url.startswith("sqlite:///./"):
        _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        rel = url[len("sqlite:///./"):]
        return f"sqlite:///{os.path.join(_BASE, rel)}"
    return url


_db_url = _resolve_db_url(settings.DATABASE_URL)
engine = create_engine(
    _db_url,
    connect_args={"check_same_thread": False} if "sqlite" in _db_url else {},
    echo=settings.DEBUG,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from database.models import Task
    Base.metadata.create_all(bind=engine)