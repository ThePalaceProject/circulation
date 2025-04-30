from celery import shared_task

from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.library import Library


@shared_task(queue=QueueNames.default, bind=True)
def update_novelists_for_all_libraries(task: Task) -> None:
    with task.session() as session:
        libraries = session.query(Library).all()
        for library in libraries:
            update_novelists_by_library.delay(library_id=library.id)
            task.log.info(
                f"Queued update task for library('{library.name}' (id={library.id})"
            )

        task.log.info(
            f"update_novelists_for_all_libraries task completed successfully."
        )


@shared_task(queue=QueueNames.default, bind=True)
def update_novelists_by_library(task: Task, library_id: int) -> None:

    with task.session() as session:
        library = Library.by_id(session, id=library_id)

        if not library:
            task.log.error(
                f"Library with id={library_id} not found. Unable to process task."
            )
            return

        api = NoveListAPI.from_config(library)

        task.log.info(
            f"Beginning update for library('{library.name}' (id={library.id})"
        )
        response = api.put_items_novelist(library)
        task.log.info(
            f"Update complete for  library('{library.name}' (id={library.id}). "
            f"Novelist API Response:\n{response}"
        )
