from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# Deterministic constraint naming convention for Alembic migrations.
# Explicit CHECK constraint names are defined in model __table_args__ to avoid double-prefixing.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Shared declarative base for all warehouse ORM models."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)