from log_parser.execution_log import VMLog, Workflow, TaskLog, TransferLog
from log_parser.execution_log import ExecutionLog, EventType


def float_or_none(string_float):
    if string_float == "None":
        return None
    else:
        return float(string_float)


def read_log(file_content):
    lines = file_content.splitlines()
    current_line = 0

    vm_number = int(lines[current_line])
    current_line += 1

    vms = {}

    for i in xrange(0, vm_number):
        vm_info = lines[current_line].split()

        vm = VMLog(id=vm_info[0], started=float_or_none(vm_info[1]), finished=float_or_none(vm_info[2]))
        vms[vm.id] = vm

        current_line += 1

    workflows_number = int(lines[current_line])
    current_line += 1

    workflows = {}

    for i in xrange(0, workflows_number):
        workflow_info = lines[current_line].split()
        workflow = Workflow(id=workflow_info[0], priority=int(workflow_info[1]), filename=workflow_info[2])
        workflows[workflow.id] = workflow
        current_line += 1

    tasks_number = int(lines[current_line])
    current_line += 1

    tasks = []

    for i in xrange(0, tasks_number):
        task_info = lines[current_line].split()
        task = TaskLog(id=task_info[0], workflow=task_info[1], task_id=task_info[2], vm=task_info[3],
            started=float_or_none(task_info[4]), finished=float_or_none(task_info[5]), result=task_info[6])
        tasks.append(task)
        current_line += 1

    transfers_number = int(lines[current_line])
    current_line += 1

    transfers = []

    for i in xrange(0, transfers_number):
        transfer_info = lines[current_line].split()
        transfer = TransferLog(id=transfer_info[0], vm=transfer_info[1], started=float_or_none(transfer_info[2]),
            finished=float_or_none(transfer_info[3]), direction=transfer_info[4],
            job_id=transfer_info[5], file_id=transfer_info[6])
        transfers.append(transfer)
        current_line += 1

    execution_log = ExecutionLog()

    for task in tasks:
        execution_log.add_event(EventType.TASK, task)

    for transfer in transfers:
        execution_log.add_event(EventType.TRANSFER, transfer)

    for vm in vms.values():
        execution_log.add_event(EventType.VM, vm)

    for workflow in workflows.values():
        execution_log.add_workflow(workflow)

    return execution_log
