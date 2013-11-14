from itertools import groupby
from operator import attrgetter
import sys
from log_parser.execution_log import EventType
from validation import parsed_log_loader
from validation.order_validator import ValidationResult

ENDS, STARTS = range(2)


def generate_job_events_sequentially(jobs):
    events = []

    for job in jobs:
        events.append((job.started, STARTS, job))
        events.append((job.finished, ENDS, job))

    return sorted(events)


def get_intersections(started_jobs, current_job):
    return [(current_job, started_job) for started_job in started_jobs]


def get_intersecting_jobs(jobs):
    intersecting_jobs = []

    started_jobs = set()
    for (_, event_type, job) in generate_job_events_sequentially(jobs):
        if event_type == STARTS:
            intersections = get_intersections(started_jobs, job)
            intersecting_jobs.extend(intersections)
            started_jobs.add(job)
        else:
            started_jobs.remove(job)

    return intersecting_jobs


def are_events_intersected(event1, event2):
    return not (event1.finished < event2.started
                or event2.finished < event1.started)


def get_intersecting_with(transfer, jobs):
    return [job for job in jobs if are_events_intersected(transfer, job)]


def group_by_dict(list_to_group, getter):
    list_to_group = sorted(list_to_group, key=getter)
    return {key: list(sub_list) for key, sub_list in groupby(list_to_group, getter)}


def validate(jobs, transfers):
    jobs_by_vm = group_by_dict(jobs, attrgetter('vm'))
    transfers_by_vm = group_by_dict(transfers, attrgetter('vm'))

    errors = []

    vms = set(jobs_by_vm.keys()) | set(transfers_by_vm.keys())

    for vm in vms:
        vm_jobs = jobs_by_vm[vm] if vm in jobs_by_vm else []
        vm_transfers = transfers_by_vm[vm] if vm in transfers_by_vm else []

        intersecting_jobs = get_intersecting_jobs(vm_jobs)

        vm_errors = ['Job {} was executed in the same time as job {} on the same VM'.format(job1.id, job2.id)
                     for (job1, job2) in intersecting_jobs]

        errors.extend(vm_errors)

        for transfer in vm_transfers:
            intersecting_jobs = get_intersecting_with(transfer, vm_jobs)

            vm_errors = ['Transfer {} was executed in the same time as job {} on the same VM'.format(
                transfer.id, job.id) for job in intersecting_jobs]

            errors.extend(vm_errors)

    return ValidationResult(errors)


# TODO(mequrel): unify this script with other validators and minimize code duplication

def main():
    if len(sys.argv) != 2:
        print('Invalid number of params. 1 param expected (filename).')
        return

    filename = sys.argv[1]
    infile = open(filename, 'r')
    execution_log = parsed_log_loader.read_log(infile.read())
    infile.close()

    jobs = execution_log.events[EventType.TASK]
    transfers = execution_log.events[EventType.TRANSFER]

    result = validate(jobs, transfers)

    for error in result.errors:
        print(error)


if __name__ == '__main__':
    main()

