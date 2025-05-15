import importlib
from pathlib import Path


def import_celery_tasks() -> None:
    """
    Import all the Celery tasks from the tasks module.

    This automatically imports all the tasks from the tasks module so that they are registered
    with the worker when it starts up.

    This makes the assumption that all of our Celery tasks will be in the `tasks` module.
    """
    tasks_path = Path(__file__).parent
    for task_file in tasks_path.glob("*.py"):
        if task_file.stem == "__init__":
            continue
        module = f"{__name__}.{task_file.stem}"
        importlib.import_module(module)


import_celery_tasks()
