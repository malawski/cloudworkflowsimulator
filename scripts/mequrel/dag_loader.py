import re

# TODO(mequrel): should fail if dag file is incorrect


FILE_PATTERN = r'FILE\s+(\w|.+)\s+(\d+)'
TASK_PATTERN = r'TASK\s+(\w+)\s+(\w+)\s+(\w+)'
EDGE_PATTERN = r'EDGE\s+(\w+)\s+(\w+)'
INPUTS_PATTERN = r'INPUTS\s+(\w+)\s+(.+)'
OUTPUTS_PATTERN = r'OUTPUTS\s+(w+)\s+(.+)'

class ComparableByAttributes(object):
    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

class DagFile(ComparableByAttributes):
    def __init__(self, filename, size):
        self.filename = filename
        self.size = size

class DagTask(ComparableByAttributes):
    def __init__(self, id, type, makespan):
        self.id = id
        self.type = type
        self.makespan = makespan

class Dag(object):
    def __init__(self):
        self.files = []
        self.tasks = []

    def get_files(self):
        return self.files

    def get_tasks(self):
        return self.tasks


def parse_file_line(line):
    match = re.match(FILE_PATTERN, line)
    if not match:
        return None

    filename = match.group(1)
    size = int(match.group(2))
    return DagFile(filename, size)

def parse_task_line(line):
    match = re.match(TASK_PATTERN, line)
    if not match:
        return None

    id = match.group(1)
    type = match.group(2)
    makespan = float(match.group(3))
    return DagTask(id, type, makespan)

def parse_dag(dag_file_content):
    result = Dag()
    for line in dag_file_content.splitlines():
        dag_file = parse_file_line(line)

        if dag_file:
            result.files.append(dag_file)
            continue

        dag_task = parse_task_line(line)

        if dag_task:
            result.tasks.append(dag_task)
            continue



    return result
