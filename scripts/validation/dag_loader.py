import re
import workflow

# TODO(mequrel): should fail if dag file is incorrect


FILE_PATTERN = r'FILE\s+(\w|.+)\s+(\d+)'
TASK_PATTERN = r'TASK\s+(\w+)\s+(\S+)\s+(\w+)'
EDGE_PATTERN = r'EDGE\s+(\w+)\s+(\w+)'
INPUTS_PATTERN = r'INPUTS\s+(\w+)\s+(.+)'
OUTPUTS_PATTERN = r'OUTPUTS\s+(w+)\s+(.+)'


def parse_file_line(line):
    match = re.match(FILE_PATTERN, line)
    if not match:
        return None

    filename = match.group(1)
    size = int(match.group(2))
    return workflow.File(filename, size)


def parse_task_line(line):
    match = re.match(TASK_PATTERN, line)
    if not match:
        return None

    id = match.group(1)
    type = match.group(2)
    makespan = float(match.group(3))
    return workflow.Task(id, makespan, type)


def parse_edge_line(line):
    match = re.match(EDGE_PATTERN, line)
    if not match:
        return None

    before_id = match.group(1)
    after_id = match.group(2)
    return before_id, after_id


def parse_dag(dag_file_content):
    dag_builder = workflow.DagBuilder()

    for line in dag_file_content.splitlines():
        dag_file = parse_file_line(line)

        if dag_file:
            dag_builder.add_file(dag_file)
            continue

        dag_task = parse_task_line(line)

        if dag_task:
            dag_builder.add_task(dag_task)
            continue

        dag_edge = parse_edge_line(line)

        if dag_edge:
            before_id, after_id = dag_edge
            dag_builder.add_edge(before_id, after_id)

    return dag_builder.build()
