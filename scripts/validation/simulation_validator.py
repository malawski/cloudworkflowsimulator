"""
Validates if simulator was behaving properly during the experiment. Raises error if:
  * two jobs execution makespan intersects each other on the same VM
  * a transfer was done at the same time and same VM when a job was executed
  * a job/transfer was executed on non-existing VM
  * a job/transfer was executed out of VM lifecycle
"""

from itertools import groupby
from operator import attrgetter
from log_parser.execution_log import EventType
from validation.common import ValidationResult

JOB_ENDS, TRANSFER_ENDS, TRANSFER_STARTS, JOB_STARTS = range(4)


def generate_events_sequentially(jobs, transfers):
    events = []

    for job in jobs:
        events.append((job.started, JOB_STARTS, job))
        events.append((job.finished, JOB_ENDS, job))

    for transfer in transfers:
        events.append((transfer.started, TRANSFER_STARTS, transfer))
        events.append((transfer.finished, TRANSFER_ENDS, transfer))

    return sorted(events)


def get_intersections(started_jobs, current_event):
    return [(current_event, started_job) for started_job in started_jobs]


def get_intersecting_events(jobs, transfers):
    intersecting_jobs = []
    intersecting_transfers = []

    started_jobs = set()
    for (_, event_type, event) in generate_events_sequentially(jobs, transfers):
        if event_type == JOB_STARTS:
            intersections = get_intersections(started_jobs, event)
            intersecting_jobs.extend(intersections)
            started_jobs.add(event)
        elif event_type == JOB_ENDS:
            if event in started_jobs:
                started_jobs.remove(event)
        elif event_type == TRANSFER_STARTS:
            intersections = get_intersections(started_jobs, event)
            intersecting_transfers.extend(intersections)
        else:
            pass

    return intersecting_jobs, intersecting_transfers


def group_by_dict(list_to_group, getter):
    list_to_group = sorted(list_to_group, key=getter)
    return {key: list(sub_list) for key, sub_list in groupby(list_to_group, getter)}


def is_fully_within(event, vm):
    return vm.started <= event.started and event.finished <= vm.finished


def get_events_out_of_lifecycle(events, vm):
    return [event for event in events if not is_fully_within(event, vm)]


def validate(jobs, transfers, vms):
    """

    Checks if simulation was done according to reality.

    :param jobs: list
    :param transfers: list
    :param vms: list
    :return: ValidationResult
    """
    jobs_by_vm = group_by_dict(jobs, attrgetter('vm'))
    transfers_by_vm = group_by_dict(transfers, attrgetter('vm'))
    vms = {vm.id: vm for vm in vms}

    errors = []

    jobs_vm_ids = set(jobs_by_vm.keys()) | set(transfers_by_vm.keys())

    for vm_id in jobs_vm_ids:
        vm_jobs = jobs_by_vm[vm_id] if vm_id in jobs_by_vm else []
        vm_transfers = transfers_by_vm[vm_id] if vm_id in transfers_by_vm else []

        if vm_id not in vms:
            job_errors = ['Job {} was executed on non-existing VM {}'.format(job.id, vm_id)
                          for job in vm_jobs]

            transfer_errors = ['Transfer {} was executed on non-existing VM {}'.format(transfer.id, vm_id)
                               for transfer in vm_transfers]

            errors.extend(job_errors)
            errors.extend(transfer_errors)
            continue

        vm = vms[vm_id]

        out_of_lifecycle_jobs = get_events_out_of_lifecycle(vm_jobs, vm)
        job_errors = ['Job {} was executed out of VM {} lifecycle'.format(job.id, vm_id)
                      for job in out_of_lifecycle_jobs]
        errors.extend(job_errors)

        out_of_lifecycle_transfers = get_events_out_of_lifecycle(vm_transfers, vm)
        job_errors = ['Transfer {} was executed out of VM {} lifecycle'.format(transfer.id, vm_id)
                      for transfer in out_of_lifecycle_transfers]
        errors.extend(job_errors)

        intersecting_jobs, intersecting_transfers = get_intersecting_events(vm_jobs, vm_transfers)

        vm_errors = ['Job {} was executed in the same time as job {} on the same VM'.format(job1.id, job2.id)
                     for (job1, job2) in intersecting_jobs]

        errors.extend(vm_errors)

        vm_errors = ['Transfer {} was executed in the same time as job {} on the same VM'.format(
            transfer.id, job.id) for (transfer, job) in intersecting_transfers]

        errors.extend(vm_errors)

    return ValidationResult(errors)


def validate_experiment(execution_log):
    jobs = execution_log.events[EventType.TASK]
    transfers = execution_log.events[EventType.TRANSFER]
    vms = execution_log.events[EventType.VM]

    return validate(jobs, transfers, vms)

