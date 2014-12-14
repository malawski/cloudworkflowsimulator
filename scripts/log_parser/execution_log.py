from collections import namedtuple
import StringIO

TaskLog = namedtuple('TaskLog', 'id workflow task_id vm started finished result')
TransferLog = namedtuple('TransferLog', 'id vm started finished direction job_id file_id')
VMLog = namedtuple('VMLog', 'id started finished')
Workflow = namedtuple('Workflow', 'id priority filename')
StorageState = namedtuple('StorageState', 'time readers_number writers_number read_speed write_speed')


# TODO(mequrel): that is bad, will fix
class EventType(object):
    TASK, TRANSFER, VM, STORAGE_STATE = range(4)


class ExecutionLog(object):
    def __init__(self):
        self.events = {EventType.TASK: [], EventType.TRANSFER: [], EventType.VM: [], EventType.STORAGE_STATE: []}

        self.workflows = []
        self.settings = None

    def add_event(self, type, event):
        if not (type == EventType.TASK and event.started is None):
          self.events[type].append(event)

    def add_workflow(self, workflow):
        self.workflows.append(workflow)

    @property
    def completed_jobs(self):
        tasks = self.events[EventType.TASK]
        return [task for task in tasks if 'OK' in task.result]

    def dumps(self):
        output = StringIO.StringIO()

        output.write('{} {} {}\n'.format(self.settings.deadline, self.settings.budget, self.settings.vm_cost_per_hour))

        output.write('{}\n'.format(len(self.events[EventType.VM])))
        for vm_event in self.events[EventType.VM]:
            output.write('{} {} {}\n'.format(vm_event.id, vm_event.started, vm_event.finished))

        output.write('{}\n'.format(len(self.workflows)))
        for workflow in self.workflows:
            output.write('{} {} {}\n'.format(workflow.id, workflow.priority, workflow.filename))

        output.write('{}\n'.format(len(self.events[EventType.TASK])))
        for task_event in self.events[EventType.TASK]:
            output.write(
                '{} {} {} {} {} {} {}\n'.format(task_event.id, task_event.workflow, task_event.task_id, task_event.vm,
                    task_event.started, task_event.finished, task_event.result))

        output.write('{}\n'.format(len(self.events[EventType.TRANSFER])))
        for transfer_event in self.events[EventType.TRANSFER]:
            output.write('{} {} {} {} {} {} {}\n'.format(transfer_event.id, transfer_event.vm, transfer_event.started,
                transfer_event.finished, transfer_event.direction,
                transfer_event.job_id, transfer_event.file_id))

        output.write('{}\n'.format(len(self.events[EventType.STORAGE_STATE])))
        for storage_state in self.events[EventType.STORAGE_STATE]:
            output.write('{} {} {} {} {}\n'.format(storage_state.time, storage_state.readers_number,
                storage_state.writers_number, storage_state.read_speed,
                storage_state.write_speed))

        contents = output.getvalue()
        output.close()
        return contents
