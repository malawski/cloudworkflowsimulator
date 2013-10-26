import itertools

TASK_STARTED = 1
TASK_FINISHED = 2

class TaskLog(object):
    def __init__(self):
        self.started = None
        self.finished = None

class ExecutionLog(object):
    def __init__(self):
        self.tasks = {}

    def add_event(self, type, id, timestamp):
        if not id in self.tasks:
            task_log = TaskLog()
            self.tasks[id] = task_log
        else:
            task_log = self.tasks[id]

        if type == TASK_STARTED:
            task_log.started = timestamp
        elif type == TASK_FINISHED:
            task_log.finished = timestamp

    def get_log_for(self, task):
        return self.tasks[task.id]



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
        task_log = execution_log.get_log_for(task)
        following_task_log = execution_log.get_log_for(following_task)
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

