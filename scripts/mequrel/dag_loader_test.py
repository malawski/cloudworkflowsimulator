import unittest
import dag_loader

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
            dag_loader.DagFile('in1.txt', 100),
            dag_loader.DagFile('in2.txt', 100)
        ]

        self.assertListEqual(expected_files, dag.get_files())

    def test_should_load_task_definitions(self):
        dag_content = '''
TASK ID000 preprocess 10.0
TASK ID001 process 10.0
        '''

        dag = dag_loader.parse_dag(dag_content)
        expected_files = [
            dag_loader.DagTask('ID000', 'preprocess', 10.0),
            dag_loader.DagTask('ID001', 'process', 10.0)
        ]

        self.assertListEqual(expected_files, dag.get_tasks())

    def test_should_load_edges_definition(self):
        pass
#        dag_content = '''
#TASK ID000 preprocess 10.0
#TASK ID001 process 10.0
#TASK ID002 postprocess 10.0
#
#EDGE ID000 ID001
#EDGE ID001 ID002
#        '''
#        #TODO(mequrel): hashmap?
#        dag = dag_loader.parse_dag(dag_content)
#        expected_files = [
#            dag_loader.DagTask('ID000', 'preprocess', 10.0),
#            dag_loader.DagTask('ID001', 'process', 10.0)
#        ]
#
#        self.assertListEqual(expected_files, dag.get_tasks())


if __name__ == '__main__':
    unittest.main()

