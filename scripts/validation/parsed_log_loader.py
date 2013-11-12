from collections import namedtuple

TaskLog = namedtuple('TaskLog', 'id workflow task_id vm started finished result')
TransferLog = namedtuple('TransferLog', 'id vm started finished direction')
VMLog = namedtuple('VMLog', 'id started finished')
Workflow = namedtuple('Workflow', 'id priority')


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
        workflow = Workflow(id=workflow_info[0], priority=int(workflow_info[1]))
        workflows[workflow.id] = workflow
        current_line += 1

    tasks_number = int(lines[current_line])
    current_line += 1

    tasks = []

    for i in xrange(0, tasks_number):
        task_info = lines[current_line].split()
        task = TaskLog(workflow=task_info[0], id=task_info[1], task_id="not_given", vm=task_info[2],
                       started=float_or_none(task_info[3]), finished=float_or_none(task_info[4]), result=task_info[5])
        tasks.append(task)
        current_line += 1

    transfers_number = int(lines[current_line])
    current_line += 1

    transfers = []

    for i in xrange(0, transfers_number):
        transfer_info = lines[current_line].split()
        transfer = TransferLog(id=transfer_info[0], vm=transfer_info[1], started=float_or_none(transfer_info[2]),
                               finished=float_or_none(transfer_info[3]), direction=transfer_info[4])
        transfers.append(transfer)
        current_line += 1

    return {
        'vms': vms,
        'workflows': workflows,
        'tasks': tasks,
        'transfers': transfers
    }
