import unittest

from scripts.validation import workflow, dag_loader


class DagLoaderTest(unittest.TestCase):
    def setUp(self):
        super(DagLoaderTest, self).setUp()
        pass

    def tearDown(self):
        super(DagLoaderTest, self).tearDown()
        pass

    def test_should_load_files_definitions(self):
        dag_content = '''
FILE in1.txt 100
FILE in2.txt 100
        '''

        dag = dag_loader.parse_dag(dag_content)
        expected_files = [
            workflow.File('in1.txt', 100),
            workflow.File('in2.txt', 100)
        ]

        self.assertListEqual(expected_files, dag.files)

    def test_should_load_task_definitions(self):
        dag_content = '''
TASK ID000 preprocess 10.0
TASK ID001 process 10.0
        '''

        dag = dag_loader.parse_dag(dag_content)
        expected_tasks = [
            workflow.Task('ID000', 10.0, 'preprocess'),
            workflow.Task('ID001', 10.0, 'process')
        ]

        self.assertListEqual(expected_tasks, dag.tasks)

    def test_should_load_task_definitions_with_weird_names(self):
        dag_content = '''
TASK ID000 Genome::filterContams_chr21:1.0 10.0
TASK ID001 Genome::filterContams_chr21:1.0 10.0
        '''

        dag = dag_loader.parse_dag(dag_content)
        expected_tasks = [
            workflow.Task('ID000', 10.0, 'Genome::filterContams_chr21:1.0'),
            workflow.Task('ID001', 10.0, 'Genome::filterContams_chr21:1.0')
        ]

        self.assertListEqual(expected_tasks, dag.tasks)


    def test_should_load_edges_definition(self):
        dag_content = '''
TASK ID000 preprocess 10.0
TASK ID001 process 10.0
TASK ID002 postprocess 10.0

EDGE ID000 ID001
EDGE ID001 ID002
        '''
        dag = dag_loader.parse_dag(dag_content)

        task_id000 = next((task for task in dag.tasks if task.id == 'ID000' ))
        task_id001 = next((task for task in dag.tasks if task.id == 'ID001' ))
        task_id002 = next((task for task in dag.tasks if task.id == 'ID002' ))

        self.assertIn(task_id001, task_id000.after)
        self.assertIn(task_id000, task_id001.before)

        self.assertIn(task_id002, task_id001.after)
        self.assertIn(task_id001, task_id002.before)


if __name__ == '__main__':
    unittest.main()

