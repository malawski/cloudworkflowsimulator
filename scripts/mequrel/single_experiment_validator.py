"""

Script validating if the experiment meets simple straightforward conditions like:
  * task started after ended
  * computation part started before task inputs were downloaded
  * etc

Expected input is the same as output format of script prepare_data_for_gantt.py

"""

import sys


def main():
    if len(sys.argv) != 2:
        print('Invalid number of params. 1 param expected (filename).')
        return

    filename = sys.argv[1]
    infile = open(filename, 'r')

    for line in infile:
        task = parse(line)
        result = validate_task(task)

        if not result.is_valid:
            print(result.message)

    infile.close()

#TODO(mequrel): Maybe should change output to null or something more meaningful.
MISSING_VALUE = -1.0

class TaskValidationResult(object):
    def __init__(self, is_valid, message = ''):
        self.is_valid = is_valid
        self.message = message

def validate_task(task):
    if task.task_start == MISSING_VALUE:
        return TaskValidationResult(False, 'task {} hasn\'t started at all'.format(task.id))

    if task.task_end == MISSING_VALUE:
        return TaskValidationResult(False, 'task {} hasn\'t finished at all'.format(task.id))

    if task.computation_start == MISSING_VALUE:
        return TaskValidationResult(False, 'task {} hasn\'t started computation at all'.format(task.id))

    if task.computation_end == MISSING_VALUE:
        return TaskValidationResult(False, 'task {} hasn\'t finished computation at all'.format(task.id))

    if not task.task_start <= task.computation_start <= task.computation_end <= task.task_end:
        return TaskValidationResult(False, 'task {} doesn\'t hold time order'.format(task.id))

    return TaskValidationResult(True)

class InvalidInputFormatException(Exception):
    pass


class Task:
    def __init__(self):
        self.vm = None
        self.id = None
        self.task_start = None
        self.computation_start = None
        self.computation_end = None
        self.task_end = None


def parse(line):
    split_line = line.split(' ')
    if len(split_line) != 6:
        raise InvalidInputFormatException()

    id, vm, task_start, computation_start, computation_end, task_end = split_line

    task = Task()
    task.id = id
    task.vm = vm
    task.task_start = float(task_start)
    task.computation_start = float(computation_start)
    task.computation_end = float(computation_end)
    task.task_end = float(task_end)

    return task

if __name__ == '__main__':
    main()

