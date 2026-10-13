from contextlib import contextmanager
from typing import Generator

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from core.config import get_settings
from database.models.base import Base

settings = get_settings()

# Re-exported so callers have one obvious import site for the metadata.
__all__ = [
    "Base",
    "DATABASE_URL",
    "SessionLocal",
    "drop_db",
    "engine",
    "get_db",
]

DATABASE_URL = settings.database_url

engine: Engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Yield a session, committing on clean exit and rolling back on error."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def drop_db() -> None:
    """Tear the warehouse down to a true pre-Alembic state.

    ``alembic_version`` is not part of ``Base.metadata``, so dropping the
    tables alone would leave a version stamp claiming the schema is at head
    while none of it exists. The next ``alembic upgrade head`` would then no-op
    and every later migration would apply on top of a missing schema, so the
    stamp table goes too. The result matches ``alembic downgrade base``.
    """
    Base.metadata.drop_all(bind=engine)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS alembic_version"))
