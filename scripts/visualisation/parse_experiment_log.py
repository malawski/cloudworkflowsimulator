from collections import namedtuple
from itertools import groupby
from operator import attrgetter
import sys
import StringIO

import log_parser

TaskLog = namedtuple('TaskLog', 'workflow id vm started finished result')
TransferLog = namedtuple('TransferLog', 'id vm started finished direction')
VMLog = namedtuple('VMLog', 'id started finished')
Workflow = namedtuple('Workflow', 'id priority')

TaskStartedEvent = namedtuple('TaskStartedEvent', 'id vm timestamp')
TaskFinishedEvent = namedtuple('TaskFinishedEvent', 'id vm timestamp')

TransferStartedEvent = namedtuple('TransferStartedEvent', 'id timestamp')
TransferFinishedEvent = namedtuple('TransferFinishedEvent', 'id timestamp')

PATTERNS = [
    # log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\w+)\)\s+Starting job ID(?P<id>\d+) on VM (?P<vm>\d+)', type=JobStartedEvent),
    log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Starting computational part of job ID(?P<id>\d+) on VM (?P<vm>\d+)', type=TaskStartedEvent),
    log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Computational part of job ID(?P<id>\d+) on VM (?P<vm>\d+) finished', type=TaskFinishedEvent),    

    log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Global transfer started: (?P<id>(\w|\.)+), size: (\d+)', type=TransferStartedEvent),    
    log_parser.Pattern(regex=r'\d+.\d+ \((?P<timestamp>\d+.\d+)\)\s+Global transfer finished: (?P<id>(\w|\.)+), bytes transferred: (\d+), duration: (\d+.\d+)', type=TransferFinishedEvent),    
]

def get_task_log_from_events(events):
    started = None
    finished = None

    for event in events:
        if isinstance(event, TaskStartedEvent):
            started = event.timestamp
            vm = event.vm
            task_id = event.id
        elif isinstance(event, TaskFinishedEvent):
            finished = event.timestamp
            vm = event.vm
            task_id = event.id

    return TaskLog(workflow='default', id=task_id, vm=vm, started=started, finished=finished, result='OK')

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
            output.write('{} {} {} {} {} {}\n'.format(task_event.workflow, task_event.id, task_event.vm, task_event.started, task_event.finished, task_event.result))

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
    log.add_workflow(Workflow(id='default', priority=20))

    task_events = [event for event in events if isinstance(event, TaskStartedEvent) or isinstance(event, TaskFinishedEvent)]

    task_events = sorted(task_events, key=attrgetter('id'))
    for task_id, group in groupby(task_events, attrgetter('id')):
        task_log = get_task_log_from_events(group)
        
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