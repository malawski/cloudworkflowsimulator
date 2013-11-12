import itertools
import sys

import parsed_log_loader
import dag_loader


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


def _validate_task_order(workflow_id, task, tasks):
    errors = []

    if not (workflow_id, task.id) in tasks:
    #     errors.append('Task {} in workflow {} was not completed at all'.format(task.id, workflow_id))
        return ValidationResult(errors)

    task_log = tasks[(workflow_id, task.id)]

    for following_task in task.after:
        if (workflow_id, following_task.id) in tasks:
            following_task_log = tasks[(workflow_id, following_task.id)]

            if not _is_done_before(task_log, following_task_log):
                errors.append('Task {} in workflow {} was not done before task {} finished'.format(
                    following_task.id, workflow_id, task.id))

    return ValidationResult(errors)


def flatten_list(nested_list):
    return list(itertools.chain.from_iterable(nested_list))


def validate(dag, tasks):
    errors = []

    for task in dag.tasks:
        result = _validate_task_order(dag.id, task, tasks)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)
    return ValidationResult(errors)


def main():
    if len(sys.argv) != 2:
        print('Invalid number of params. 1 param expected (filename).')
        return

    filename = sys.argv[1]
    infile = open(filename, 'r')
    execution_log = parsed_log_loader.read_log(infile.read())
    infile.close()

    tasks = execution_log.tasks_for_dag

    errors = []
    for workflow in execution_log.workflows:
        dag_file = open(workflow.filename, 'r')
        dag = dag_loader.parse_dag(dag_file.read())
        dag.id = workflow.id
        dag_file.close()

        result = validate(dag, tasks)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)

    for error in errors:
        print(error)


if __name__ == '__main__':
    main()

