"""
Script parsing cws detailed log in order to
prepare convenient input for gantt chart generator and validators

Input format:

(...)
0.0 (37.250810595555556) Global transfer started: chr21.2.8.sfq, size: 18605592
1.3055099544444388 (38.556320549999995) Global transfer finished: chr21.1.0.contam.sfq, bytes transferred: 16745453, duration: 0.66981812
0.0 (38.587238909999996)  Job ID00007 finished on VM 12
0.0 (38.587238909999996)  Starting job 67 on VM 12
0.0 (38.587238909999996) Global transfer started: chr21.1.2.sfq, size: 37006219
0.0 (39.05000127555556)  Starting job 47 on VM 24
(...)

Output format [jobId, VM, startTime, endTime]:

00007 12 32.587238909999996 38.587238909999996
00017 4 36.587238909999996 39.587238909999996

"""

import sys
import re


def main():
    filename = sys.argv[1]
    infile = open(filename, "r")

    for line in infile:
        maybeStartedTask(line)
        maybeFinishedTask(line)

    infile.close()

    printTasks();


startedTaskPattern = r"\d+.\d+ \((\d+.\d+)\)\s+Starting job ID(\d+) on VM (\d+)"
finishedTaskPattern = r"\d+.\d+ \((\d+.\d+)\)\s+Job ID(\d+) finished on VM (\d+)"

tasks = {}

def printTasks():
    for id in tasks:
        task = tasks[id]

        print ("{} {} {} {}".format(id, task[0], task[1], task[2]))


def createTaskIfNotExists(task):
    if not task in tasks:
        tasks[task] = [None, -1, -1]


def maybeStartedTask(line):
    match = re.match(startedTaskPattern, line)

    if match:
        time = match.group(1)
        id = match.group(2)
        vm = match.group(3)

        createTaskIfNotExists(id)

        tasks[id][0] = vm
        tasks[id][1] = time

    return bool(match)


def maybeFinishedTask(line):
    match = re.match(finishedTaskPattern, line)

    if match:
        time = match.group(1)
        id = match.group(2)
        vm = match.group(3)

        createTaskIfNotExists(id)

        tasks[id][0] = vm
        tasks[id][2] = time

    return bool(match)

if __name__ == "__main__":
    main();



