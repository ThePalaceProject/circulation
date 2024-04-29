import celery


class Celery(celery.Celery):
    def gen_task_name(self, name: str, module: str):
        """
        This method is used to generate the task name for the Celery task.

        The default implementation is repetitive for our use case, because all our tasks
        live in the `palace.manager.celery.tasks` module. This method removes that prefix
        from the task name to make it more readable.
        """
        module = module.removeprefix("palace.manager.celery.tasks.")
        return super().gen_task_name(name, module)
