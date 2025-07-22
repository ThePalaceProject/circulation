from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.metadata.nyt import NYTBestSellerAPI
from palace.manager.service.celery.celery import QueueNames


@shared_task(queue=QueueNames.default, bind=True)
def update_nyt_best_sellers_lists(task: Task, include_history: bool = False) -> None:

    with task.session() as session:
        try:
            api = NYTBestSellerAPI.from_config(session)
        except CannotLoadConfiguration as e:
            task.log.warning(f"Skipping update: {e.message}")
            return

        names = api.list_of_lists()

    for l in sorted(names["results"], key=lambda x: x["list_name_encoded"]):
        # run each list update in its own transaction to minimize transaction time and size
        with task.transaction() as session:
            api = NYTBestSellerAPI.from_config(session)
            name = l["list_name_encoded"]
            task.log.info(f"Handling list {name}")
            best = api.best_seller_list(l)

            if include_history:
                api.fill_in_history(best)
            else:
                api.update(best)

            # Mirror the list to the database.
            custom_list = best.to_customlist(session)
            task.log.info(
                f'Now custom list named "{custom_list.name}" '
                f"contains {len(custom_list.entries)} entries in the list."
            )
