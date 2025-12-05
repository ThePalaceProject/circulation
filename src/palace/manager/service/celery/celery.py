from enum import StrEnum, auto
from typing import Any

from celery.schedules import crontab
from kombu import Exchange, Queue

from palace.manager.celery.celery import Celery


class QueueNames(StrEnum):
    high = auto()
    """
    The `high` queue is for tasks that are short running and should be processed quickly because
    they are time-sensitive (usually a user is waiting for them).
    """

    default = auto()
    """
    The `default` queue is for tasks that are longer running and can be processed when the worker
    has capacity.
    """

    apply = auto()
    """
    The `apply` queue is a special queue for BibliographicData and CirculationData updates. These
    are individually small tasks, and they aren't time-sensitive, but they are extremely high volume,
    so we want to keep them separate from the other tasks.
    """


def beat_schedule() -> dict[str, Any]:
    """
    Configure the Celery beat schedule.

    This is a dictionary of tasks that should be run periodically. The key is the schedule name, and
    the value is a dictionary of options for the task. The `schedule` key is required and should
    be a timedelta object or a crontab string. The `task` key is required and should be the name
    of the task to run.
    """
    from palace.manager.celery.tasks import (
        boundless,
        marc,
        notifications,
        novelist,
        nyt,
        opds1,
        opds2,
        opds_for_distributors,
        opds_odl,
        playtime_entries,
        reaper,
        rotate_jwe_key,
        saml,
        search,
        work,
    )

    return {
        "full_search_reindex": {
            "task": search.search_reindex.name,
            "schedule": crontab(hour="0", minute="10"),  # Run every day at 12:10 AM
        },
        "search_indexing": {
            "task": search.search_indexing.name,
            "schedule": crontab(minute="*"),  # Run every minute
        },
        "marc_export": {
            "task": marc.marc_export.name,
            "schedule": crontab(
                hour="3,11", minute="0"
            ),  # Run twice a day at 3:00 AM and 11:00 AM
        },
        "marc_export_cleanup": {
            "task": marc.marc_export_cleanup.name,
            "schedule": crontab(
                hour="1",
                minute="0",
            ),  # Run every day at 1:00 AM
        },
        "opds2_odl_remove_expired_holds": {
            "task": opds_odl.remove_expired_holds.name,
            "schedule": crontab(
                minute="16",
            ),  # Run every hour at 16 minutes past the hour
        },
        "opds2_odl_recalculate_hold_queue": {
            "task": opds_odl.recalculate_hold_queue.name,
            "schedule": crontab(
                minute="31",
            ),  # Run every hour at 31 minutes past the hour
        },
        "opds2_odl_import_all": {
            "task": opds_odl.import_all.name,
            "schedule": crontab(
                minute="45", hour="*/3"
            ),  # Run every 3 hours at 45 minutes past the hour
        },
        "rotate_jwe_key": {
            "task": rotate_jwe_key.rotate_jwe_key.name,
            "schedule": crontab(
                minute="0",
                hour="3",
            ),  # Run every day at 3:00 AM
        },
        "loan_expiration_notifications": {
            "task": notifications.loan_expiration.name,
            "schedule": crontab(
                minute="*/20",
            ),  # Run every 20 minutes
        },
        "hold_available_notifications": {
            "task": notifications.hold_available.name,
            "schedule": crontab(
                minute="*/20",
            ),  # Run every 20 minutes
        },
        "boundless_import_all_collections": {
            "task": boundless.import_all_collections.name,
            "schedule": crontab(minute="*/15"),  # Run every 15 minutes
        },
        "credential_reaper": {
            "task": reaper.credential_reaper.name,
            "schedule": crontab(
                minute="5",
                hour="2",
            ),  # Once a day at 2:05 AM
        },
        "patron_reaper": {
            "task": reaper.patron_reaper.name,
            "schedule": crontab(
                minute="10",
                hour="2",
            ),  # Once a day at 2:10 AM
        },
        "collection_reaper": {
            "task": reaper.collection_reaper.name,
            "schedule": crontab(
                minute="15",
                hour="2",
            ),  # Once a day at 2:15 AM
        },
        "work_reaper": {
            "task": reaper.work_reaper.name,
            "schedule": crontab(
                minute="20",
                hour="2",
            ),  # Once a day at 2:20 AM
        },
        "measurement_reaper": {
            "task": reaper.measurement_reaper.name,
            "schedule": crontab(
                minute="25",
                hour="2",
            ),  # Once a day at 2:25 AM
        },
        "annotation_reaper": {
            "task": reaper.annotation_reaper.name,
            "schedule": crontab(
                minute="30",
                hour="2",
            ),  # Once a day at 2:30 AM
        },
        "hold_reaper": {
            "task": reaper.hold_reaper.name,
            "schedule": crontab(
                minute="35",
                hour="2",
            ),  # Once a day at 2:35 AM
        },
        "loan_reaper": {
            "task": reaper.loan_reaper.name,
            "schedule": crontab(
                minute="40",
                hour="2",
            ),  # Once a day at 2:40 AM
        },
        "removed_license_pool_hold_loan_reaper": {
            "task": reaper.removed_license_pool_hold_loan_reaper.name,
            "schedule": crontab(
                minute="24",
                hour="6",
            ),  # Every day at 6:24 AM (After OPDS2 and OPDS2+ODL imports are complete)
        },
        "reap_unassociated_loans": {
            "task": reaper.reap_unassociated_loans.name,
            "schedule": crontab(
                minute="45",
                hour="2",
            ),  # Once a day at 2:45 AM
        },
        "reap_unassociated_holds": {
            "task": reaper.reap_unassociated_holds.name,
            "schedule": crontab(
                minute="50",
                hour="2",
            ),  # Once a day at 2:50 AM
        },
        "reap_loans_in_inactive_collections": {
            "task": reaper.reap_loans_in_inactive_collections.name,
            "schedule": crontab(
                minute="55",
                hour="2",
            ),  # Once a day at 2:55 AM
        },
        "generate_playtime_report": {
            "task": playtime_entries.generate_playtime_report.name,
            "schedule": crontab(
                minute="0", hour="4", day_of_month="2"
            ),  # On the second day of the month at 4:00 AM
        },
        "sum_playtime_entries": {
            "task": playtime_entries.sum_playtime_entries.name,
            "schedule": crontab(
                minute="0",
                hour="8,20",
            ),  # Every 12 hours, but spaced after hour 8 to reduce job cluttering
        },
        "update_nyt_best_sellers_lists": {
            "task": nyt.update_nyt_best_sellers_lists.name,
            "schedule": crontab(
                minute="30",
                hour="3",
            ),  # Every morning at 3:30 am.
        },
        "update_novelists_for_all_libraries": {
            "task": novelist.update_novelists_for_all_libraries.name,
            "schedule": crontab(
                minute="0", hour="0", day_of_week="0"
            ),  # Every Sunday at midnight
        },
        "update_saml_federation_idps_metadata": {
            "task": saml.update_saml_federation_idps_metadata.name,
            "schedule": crontab(
                minute="0",
                hour="5",
            ),  # Every day at 5 am
        },
        "classify_unchecked_subjects": {
            "task": work.classify_unchecked_subjects.name,
            "schedule": crontab(minute="30", hour="22"),  # Every night at 10:30pm
        },
        "opds2_import_all": {
            "task": opds2.import_all.name,
            "schedule": crontab(minute="30", hour="5"),  # Every day at 5:30 AM
        },
        "opds1_import_all": {
            "task": opds1.import_all.name,
            "schedule": crontab(minute="0", hour="5"),  # Every day at 5:00 AM
        },
        "opds_for_distributors_import_all": {
            "task": opds_for_distributors.import_all.name,
            "schedule": crontab(minute="0", hour="4"),  # Every day at 4:00 AM
        },
        "opds_for_distributors_reap_all": {
            "task": opds_for_distributors.reap_all.name,
            "schedule": crontab(
                minute="0",
                hour="0",
                day_of_week="2",
            ),  # Every Tuesday at midnight
        },
    }


def task_queue_config() -> dict[str, Any]:
    """
    Configure the task queues for our Celery app.

    See `QueueNames` for more information on what each queue is used for.

    TODO: Evaluate if we need more granular queues for different types of tasks, as we roll this
      out to production and start to monitor worker utilization.
    """
    task_queues = []
    for queue in QueueNames:
        task_queues.append(Queue(queue, Exchange(queue), routing_key=queue))

    return {
        "task_queues": task_queues,
        "task_default_queue": QueueNames.default,
        "task_default_exchange": QueueNames.default,
        "task_default_routing_key": QueueNames.default,
    }


def celery_factory(config: dict[str, Any]) -> Celery:
    """
    Create and configure our Celery app, setting it as the default so that tasks registered
    with `shared_task` will use this app.
    """

    # Create a new Celery app
    app = Celery(task_cls="palace.manager.celery.task:Task")
    app.conf.update(config)
    app.conf.update(task_queue_config())
    app.conf.update({"beat_schedule": beat_schedule()})
    app.set_default()

    return app
