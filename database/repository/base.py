from typing import Generic, Sequence, Type, TypeVar
from sqlalchemy import select
from sqlalchemy.orm import Session
from database.models.base import Base

ModelType = TypeVar("ModelType", bound=Base)


class BaseRepository(Generic[ModelType]):

    def __init__(self, model: Type[ModelType], session: Session) -> None:
        self.model = model
        self.session = session

    def get_by_id(self, id: object) -> ModelType | None:
        return self.session.get(self.model, id)

    def get_all(self, limit: int = 100, offset: int = 0) -> Sequence[ModelType]:
        stmt = select(self.model).limit(limit).offset(offset)
        return self.session.execute(stmt).scalars().all()

    def delete(self, id: object) -> bool:
        instance = self.get_by_id(id)
        if instance is None:
            return False
        self.session.delete(instance)
        return True
