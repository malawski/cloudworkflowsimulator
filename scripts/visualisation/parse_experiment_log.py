from collections import namedtuple
from itertools import groupby
from operator import attrgetter
import sys
import StringIO

import log_parser

TaskLog = namedtuple('TaskLog', 'id workflow task_id vm started finished result')
TransferLog = namedtuple('TransferLog', 'id vm started finished direction')
VMLog = namedtuple('VMLog', 'id started finished')
Workflow = namedtuple('Workflow', 'id priority')

TransferStartedEvent = namedtuple('TransferStartedEvent', 'id timestamp')
TransferFinishedEvent = namedtuple('TransferFinishedEvent', 'id timestamp')

PATTERNS = [
    log_parser.Pattern(
            regex=r'\d+.\d+ \((?P<started>\d+.\d+)\)\s+Starting computational part of job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow = (?P<workflow>\w+)\) on VM (?P<vm>\d+)',
            type=TaskLog,
            set_values={'finished': None, 'result': None}),
    log_parser.Pattern(
            regex=r'\d+.\d+ \((?P<finished>\d+.\d+)\)\s+Computational part of job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow = (?P<workflow>\w+)\) on VM (?P<vm>\d+) finished',
            type=TaskLog,
            set_values={'started': None, 'result': 'OK'}),    
    log_parser.Pattern(
            regex=r'\d+.\d+ \((?P<finished>\d+.\d+)\)\s+Job (?P<id>\d+) \(task_id = (?P<task_id>\w+), workflow_id = (?P<workflow>\w+)\) failed on VM (?P<vm>\d+). Resubmitting...',
            type=TaskLog,
            set_values={'started': None, 'result': 'FAILED'}),    

    # log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Global transfer started: (?P<id>(\w|\.)+), size: (\d+)', type=TransferStartedEvent),    
    # log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Global transfer finished: (?P<id>(\w|\.)+), bytes transferred: (\d+), duration: (\d+.\d+)', type=TransferFinishedEvent),    

    # log_parser.Pattern(regex=r'Workflow (?P<id>\w+), priority = (?P<priority>\d+), filename = (.*)', type=Workflow)
]

# TODO(mequrel): change to something more readable (comprehension list)
def merge_tuples_regarding_nones(tuple1, tuple2):
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

    return TaskLog(**result_dict)

def group_by_id(events):
    events = sorted(events, key=attrgetter('id'))
    return groupby(events, attrgetter('id'))

def glue_fissured_events(events):
    result = []
    for event_id, same_id_events in group_by_id(events):
        event = reduce(merge_tuples_regarding_nones, same_id_events)
        result.append(event)
    return result

def get_transfer_log_from_events(events):
    started = None
    finished = None

    vm = 1
    for event in events:
        if isinstance(event, TransferStartedEvent):
            started = event.timestamp
            # vm = event.vm
            file_id = event.id
        elif isinstance(event, TransferFinishedEvent):
            finished = event.timestamp
            # vm = event.vm
            file_id = event.id

    return TransferLog(id=file_id, vm=vm, started=started, finished=finished, direction='UPLOAD')

class EventType(object):
    TASK, TRANSFER, VM = range(3)

class ExecutionLog(object):
    def __init__(self):
        self.events = {}
        self.events[EventType.TASK] = []
        self.events[EventType.TRANSFER] = []
        self.events[EventType.VM] = []

        self.workflows = []

    def add_event(self, type, event):
        self.events[type].append(event)

    def add_workflow(self, workflow):
        self.workflows.append(workflow)

    def dumps(self):
        output = StringIO.StringIO()
        output.write('{}\n'.format(len(self.events[EventType.VM])))
        for vm_event in self.events[EventType.VM]:
            output.write('{} {} {}\n'.format(vm_event.id, vm_event.started, vm_event.finished))

        output.write('{}\n'.format(len(self.workflows)))
        for workflow in self.workflows:
            output.write('{} {}\n'.format(workflow.id, workflow.priority))

        output.write('{}\n'.format(len(self.events[EventType.TASK])))
        for task_event in self.events[EventType.TASK]:
            output.write('{} {} {} {} {} {}\n'.format(task_event.workflow, task_event.task_id, task_event.vm, task_event.started, task_event.finished, task_event.result))

        output.write('{}\n'.format(len(self.events[EventType.TRANSFER])))
        for transfer_event in self.events[EventType.TRANSFER]:
            output.write('{} {} {} {} {}\n'.format(transfer_event.id, transfer_event.vm, transfer_event.started, transfer_event.finished, transfer_event.direction))

        contents = output.getvalue()
        output.close()
        return contents

def main():
    filename = sys.argv[1]
    parser = log_parser.LogParser()

    for pattern in PATTERNS:
        parser.add_pattern(pattern)

    events = parser.parse(filename)

    log = ExecutionLog()

    workflows = [event for event in events if isinstance(event, Workflow)]

    for workflow in workflows:
        log.add_workflow(workflow)

    task_events = [event for event in events if isinstance(event, TaskLog)]
    task_events = glue_fissured_events(task_events)

    for task_log in task_events:
        log.add_event(EventType.TASK, task_log)

    transfer_events = [event for event in events if isinstance(event, TransferStartedEvent) or isinstance(event, TransferFinishedEvent)]


    # TODO(mequrel): That key may not be unique. Should be fixed.
    def get_transfer_key(transfer):
        return transfer.id
        # return transfer.id, transfer.vm

    transfer_events = sorted(transfer_events, key=get_transfer_key)
    for transfer_id, group in groupby(transfer_events, get_transfer_key):
        transfer_log = get_transfer_log_from_events(group)

        log.add_event(EventType.TRANSFER, transfer_log)

    print log.dumps()




if __name__ == "__main__":
    main()