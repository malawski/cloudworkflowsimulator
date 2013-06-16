"""

Script validating if the experiment meets simple straightforward conditions like:
  * task started after ended
  * computation part started before task inputs were downloaded
  * etc

Expected input is the same as output format of script prepare_data_for_gantt.py

"""

import sys


def main():
    filename = sys.argv[1]
    infile = open(filename, "r")

    for line in infile:
        task = parse(line)
        validate(task)

    infile.close()


def validate(task):
    if task.task_start == -1.0:
        print("INVALID INPUT: task {} hasn't started at all".format(task.id))
        return

    if task.task_end == -1.0:
        print("INVALID INPUT: task {} hasn't finished at all".format(task.id))
        return

    if task.computation_start == -1.0:
        print("INVALID INPUT: task {} hasn't started computation at all".format(task.id))
        return

    if task.computation_end == -1.0:
        print("INVALID INPUT: task {} hasn't finished computation at all".format(task.id))
        return

    if not ( task.task_start <= task.computation_start <= task.computation_end <= task.task_end ):
        print("INVALID INPUT: task {} doesn't hold time order".format(task.id))
        print(
        "    {} <= {} <= {} <= {}".format(task.task_start, task.computation_start, task.computation_end, task.task_end))
        return


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
    split_line = line.split(" ")
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

if __name__ == "__main__":
    main()

