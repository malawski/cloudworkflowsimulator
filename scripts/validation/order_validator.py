import itertools

import dag_loader
from log_parser.execution_log import EventType
from validation.common import ValidationResult


def _is_done_before(task1_log, task2_log):
    # this constraint is enough
    # the rest is checked in different validators
    return task1_log.finished <= task2_log.started


def _validate_task_order(workflow_id, task, jobs):
    errors = []

    if not (workflow_id, task.id) in jobs:
        return ValidationResult(errors)

    job = jobs[(workflow_id, task.id)]

    for following_task in task.after:
        if (workflow_id, following_task.id) in jobs:
            following_job = jobs[(workflow_id, following_task.id)]

            if not _is_done_before(job, following_job):
                errors.append('Task {} in workflow {} was not done before task {} finished'.format(
                    following_task.id, workflow_id, task.id))

    return ValidationResult(errors)


def _validate_transfer_order(workflow_id, task, jobs, transfers):
    def get_transfer_task_for(file_id, job_id):
        key = (file_id, job_id)
        return transfers[key] if key in transfers else None

    errors = []

    if not (workflow_id, task.id) in jobs:
        return ValidationResult(errors)

    job = jobs[(workflow_id, task.id)]

    for file_id in task.files_needed:
        transfer_job = get_transfer_task_for(file_id, job.id)

        if not transfer_job or transfer_job.vm != job.vm or transfer_job.direction != 'DOWNLOAD':
            errors.append(
                'File {} was not downloaded to VM {} despite the fact that job {} needed that as an input file'.format(
                    file_id, job.vm, job.id))
        elif not _is_done_before(transfer_job, job):
            errors.append(
                'Job {} started before file {} was transferred to VM {} despite the fact this file is its input file'.format(
                    job.id, file_id, job.vm))

    for file_id in task.files_produced:
        transfer_job = get_transfer_task_for(file_id, job.id)

        if not transfer_job or transfer_job.vm != job.vm or transfer_job.direction != 'UPLOAD':
            errors.append(
                'File {} was not transferred to VM {} despite of the fact that job {} produced that as an output file'.format(
                    file_id, job.vm, job.id))
        elif not _is_done_before(job, transfer_job):
            errors.append(
                'File {} was transferred to VM {} before job {} finished despite of the fact that this file is its output file'.format(
                    file_id, job.vm, job.id))

    return ValidationResult(errors)


def flatten_list(nested_list):
    return list(itertools.chain.from_iterable(nested_list))


def validate(dag, jobs):
    jobs = {(job.workflow, job.task_id): job for job in jobs}

    errors = []

    for task in dag.tasks:
        result = _validate_task_order(dag.id, task, jobs)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)
    return ValidationResult(errors)


def validate_transfers(dag, jobs, transfers):
    jobs = {(job.workflow, job.task_id): job for job in jobs}
    transfers = {(transfer.file_id, transfer.job_id): transfer for transfer in transfers}

    errors = []

    for task in dag.tasks:
        result = _validate_transfer_order(dag.id, task, jobs, transfers)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)

    return ValidationResult(errors)


def load_dag(workflow):
    dag_file = open(workflow.filename, 'r')
    dag = dag_loader.parse_dag(dag_file.read())
    dag.id = workflow.id
    dag_file.close()
    return dag


def validate_experiment(execution_log):
    jobs = execution_log.completed_jobs
    transfers = execution_log.events[EventType.TRANSFER]

    errors = []
    for workflow in execution_log.workflows:
        dag = load_dag(workflow)

        result = validate(dag, jobs)
        if not result.is_valid:
            errors.append(result.errors)

        result = validate_transfers(dag, jobs, transfers)
        if not result.is_valid:
            errors.append(result.errors)

    errors = flatten_list(errors)
    return ValidationResult(errors)



