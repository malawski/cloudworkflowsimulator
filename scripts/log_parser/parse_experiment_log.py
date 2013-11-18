"""
Parses raw log output from CWS to intermediate format 
that is used by validating and visualising scripts.

Example of usage:
$ python -m log_parser.parse_experiment_log raw.log preprocessed.log

"""

import argparse
from itertools import groupby
from operator import attrgetter

import log_parser
from execution_log import TaskLog, TransferLog, VMLog, Workflow, ExecutionLog, EventType
from execution_log import StorageState
from validation.common import ExperimentSettingsWithId, ExperimentSettings


PATTERNS = [
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<started>\d+.\d+)\)\s+Starting computational part of job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow = (?P<workflow>\w+)\) on VM (?P<vm>\d+)',
        type=TaskLog,
        set_values={'finished': None, 'result': None}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+Computational part of job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow = (?P<workflow>\w+), retry = false\) on VM (?P<vm>\d+) finished',
        type=TaskLog,
        set_values={'started': None, 'result': 'OK'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+Computational part of job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow = (?P<workflow>\w+), retry = true\) on VM (?P<vm>\d+) finished',
        type=TaskLog,
        set_values={'started': None, 'result': 'RETRY_OK'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+Job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow_id = (?P<workflow>\w+), retry = false\) failed on VM (?P<vm>\d+). Resubmitting...',
        type=TaskLog,
        set_values={'started': None, 'result': 'FAILED'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+Job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow_id = (?P<workflow>\w+), retry = true\) failed on VM (?P<vm>\d+). Resubmitting...',
        type=TaskLog,
        set_values={'started': None, 'result': 'RETRY_FAILED'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<started>\d+.\d+)\)\s+Global write transfer (?P<id>\d+) started: (?P<file_id>(\w|\.)+), size: (\d+), vm: (?P<vm>\d+), job_id: (?P<job_id>\d+)',
        type=TransferLog,
        set_values={'finished': None, 'direction': 'UPLOAD'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<started>\d+.\d+)\)\s+Global read transfer (?P<id>\d+) started: (?P<file_id>(\w|\.)+), size: (\d+), vm: (?P<vm>\d+), job_id: (?P<job_id>\d+)',
        type=TransferLog,
        set_values={'finished': None, 'direction': 'DOWNLOAD'}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+Global (read|write) transfer (?P<id>\d+) finished: ((\w|\.)+), bytes transferred: (\d+), duration: (\d+.\d+)',
        type=TransferLog,
        set_values={'started': None, 'vm': None, 'direction': None, 'job_id': None, 'file_id': None}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<started>\d+.\d+)\)\s+VM (?P<id>(\w|\.)+) started',
        type=VMLog,
        set_values={'finished': None}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<finished>\d+.\d+)\)\s+VM (?P<id>(\w|\.)+) terminated',
        type=VMLog,
        set_values={'started': None}),
    log_parser.Pattern(
        regex=r'Workflow (?P<id>\w+), priority = (?P<priority>\d+), filename = (?P<filename>.*)',
        type=Workflow,
        set_values={}),
    log_parser.Pattern(
        regex=r'budget = (?P<budget>\d+.\d+) (\d+.\d+) (\d+(.\d+)?)',
        type=ExperimentSettingsWithId,
        set_values={'id': 0, 'deadline': None, 'vm_cost_per_hour': 1}),
    log_parser.Pattern(
        regex=r'deadline = (?P<deadline>\d+.\d+) (\d+.\d+) (\d+(.\d+)?)',
        type=ExperimentSettingsWithId,
        set_values={'id': 0, 'budget': None, 'vm_cost_per_hour': None}),
    log_parser.Pattern(
        regex=r'\d+.\d+(E(\+|-)?\d+)? \((?P<time>\d+.\d+)\)\s+GS state has changed: readers = (?P<readers_number>\d+), writers = (?P<writers_number>\d+), read_speed = (?P<read_speed>\d+.\d+), write_speed = (?P<write_speed>\d+.\d+)',
        type=StorageState,
        set_values={}),


]


def main():
    args = parse_arguments()
    events = parse_raw_log(args.raw_log)

    log = create_execution_log_from_events(events)
    write_execution_log(log, args.output_log)


# TODO(mequrel): change to something more readable (comprehension list)
def merge_tuples_regarding_nones(tuple1, tuple2):
    tuple_type = type(tuple1)

    dict1 = tuple1.__dict__
    dict2 = tuple2.__dict__
    result_dict = {}
    for key, value in dict1.items():
        if value is None:
            if dict2[key] is None:
                result_dict[key] = None
            else:
                result_dict[key] = dict2[key]
        else:
            result_dict[key] = value

    return tuple_type(**result_dict)


def group_by_id(events):
    events = sorted(events, key=attrgetter('id'))
    return groupby(events, attrgetter('id'))


def glue_fissured_events(events):
    result = []
    for event_id, same_id_events in group_by_id(events):
        event = reduce(merge_tuples_regarding_nones, same_id_events)
        result.append(event)
    return result


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('raw_log', help='Path to file with raw experiment log (directly outputted from CWS).')
    parser.add_argument('output_log', help='Path to output file.')
    args = parser.parse_args()
    return args


def parse_raw_log(raw_log_file):
    parser = log_parser.LogParser()
    for pattern in PATTERNS:
        parser.add_pattern(pattern)
    return parser.parse(raw_log_file)


def create_execution_log_from_events(events):
    log = ExecutionLog()
    settings_logs = [event for event in events if isinstance(event, ExperimentSettingsWithId)]
    settings_logs = glue_fissured_events(settings_logs)
    settings = settings_logs[0]
    settings = ExperimentSettings(budget=settings.budget, deadline=settings.deadline,
        vm_cost_per_hour=settings.vm_cost_per_hour)
    log.settings = settings
    workflows = [event for event in events if isinstance(event, Workflow)]
    for workflow in workflows:
        log.add_workflow(workflow)
    task_events = [event for event in events if isinstance(event, TaskLog)]
    task_events = glue_fissured_events(task_events)
    for task_log in task_events:
        if task_log.finished is not None:
            log.add_event(EventType.TASK, task_log)
    transfer_events = [event for event in events if isinstance(event, TransferLog)]
    transfer_events = glue_fissured_events(transfer_events)
    for transfer_log in transfer_events:
        if transfer_log.finished is not None:
            log.add_event(EventType.TRANSFER, transfer_log)
    vm_events = [event for event in events if isinstance(event, VMLog)]
    vm_events = glue_fissured_events(vm_events)
    for vm_log in vm_events:
        log.add_event(EventType.VM, vm_log)
    storage_state_events = [event for event in events if isinstance(event, StorageState)]
    for storage_state in storage_state_events:
        log.add_event(EventType.STORAGE_STATE, storage_state)
    return log


def write_execution_log(log, output_log_file):
    with open(output_log_file, "w") as out_file:
        out_file.write(log.dumps())


if __name__ == "__main__":
    main()