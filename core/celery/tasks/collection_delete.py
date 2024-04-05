from __future__ import annotations

from celery import shared_task
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from core.celery.job import Job
from core.celery.task import Task
from core.model import Collection


class CollectionDeleteJob(Job):
    def __init__(self, session_maker: sessionmaker[Session], collection_id: int):
        super().__init__(session_maker)
        self.collection_id = collection_id

    @staticmethod
    def collection(session: Session, collection_id: int) -> Collection | None:
        return (
            session.execute(select(Collection).where(Collection.id == collection_id))
            .scalars()
            .one_or_none()
        )

    @staticmethod
    def collection_name(collection: Collection) -> str:
        return f"{collection.name}/{collection.protocol} ({collection.id})"

    def run(self) -> None:
        with self.transaction() as session:
            collection = self.collection(session, self.collection_id)
            if collection is None:
                self.log.error(
                    f"Collection with id {self.collection_id} not found. Unable to delete."
                )
                return

            self.log.info(f"Deleting collection {self.collection_name(collection)}")
            collection.delete()


@shared_task(key="high", bind=True)
def collection_delete(task: Task, collection_id: int) -> None:
    CollectionDeleteJob(task.session_maker, collection_id).run()
