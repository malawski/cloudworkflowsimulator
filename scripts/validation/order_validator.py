import itertools


class ValidationResult(object):
    def __init__(self, errors):
        self.errors = errors

    @property
    def is_valid(self):
        return len(self.errors) == 0


def _is_done_before(task1_log, task2_log):
    # this constraint is enough
    # the rest is checked in different validators
    return task1_log.finished <= task2_log.started


def _validate_task_order(task, execution_log):
    errors = []

    for following_task in task.after:
        task_log = execution_log.tasks_by_id[task.id]
        following_task_log = execution_log.tasks_by_id[following_task.id]

        if not _is_done_before(task_log, following_task_log):
            errors.append('Task {} was not done before task finished {}'.format(
                following_task, task))

    return ValidationResult(errors)


def flatten_list(nested_list):
    return list(itertools.chain.from_iterable(nested_list))


def validate(dag, execution_log):
    errors = []

    for task in dag.tasks:
        result = _validate_task_order(task, execution_log)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)
    return ValidationResult(errors)

