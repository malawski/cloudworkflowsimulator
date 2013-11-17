class ComparableByAttributes(object):
    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Task(ComparableByAttributes):
    def __init__(self, id, makespan, type=''):
        self.id = id
        self.makespan = makespan
        self.type = type

        self.after = []
        self.before = []

        self.files_needed = []
        self.files_produced = []


class File(ComparableByAttributes):
    def __init__(self, filename, size):
        self.filename = filename
        self.size = size


class Dag(object):
    def __init__(self, tasks, files):
        self.tasks = tasks
        self.files = files
        self.id = None


class DagBuilder(object):
    def __init__(self):
        self.tasks = {}
        self.files = []

    def add_task(self, task):
        self.tasks[task.id] = task

    def add_edge(self, from_id, to_id):
        from_task = self.tasks[from_id]
        to_task = self.tasks[to_id]

        from_task.after.append(to_task)
        to_task.before.append(from_task)

    def add_file(self, file):
        self.files.append(file)

    def add_input_file(self, task_id, file_id):
        self.tasks[task_id].files_needed.append(file_id)

    def add_output_file(self, task_id, file_id):
        self.tasks[task_id].files_produced.append(file_id)

    def build(self):
        return Dag(self.tasks.values(), self.files)

