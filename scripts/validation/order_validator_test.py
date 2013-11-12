import unittest

from scripts.validation import workflow, order_validator
from scripts.validation.parsed_log_loader import TaskLog

IRRELEVANT_TASK_ATTRIBUTES = {
    'workflow': '1',
    'task_id': 'some_task_id',
    'vm': 1,
    'result': 'OK'
}


class OrderValidatorTest(unittest.TestCase):
    #         /-> child1
    # parent |
    #         \-> child2
    def _prepare_parent_child_dag(self):
        parent_task = workflow.Task('parent', 10.0)
        child1_task = workflow.Task('child1', 2.0)
        child2_task = workflow.Task('child2', 3.0)

        dag_builder = workflow.DagBuilder()
        dag_builder.add_task(parent_task)
        dag_builder.add_task(child1_task)
        dag_builder.add_task(child2_task)

        dag_builder.add_edge('parent', 'child1')
        dag_builder.add_edge('parent', 'child2')

        return dag_builder.build()

    # before --> (transferred.txt) --> after
    def _prepare_file_transfer_dag(self):
        before_task = workflow.Task('before', 10.0)
        after_task = workflow.Task('after', 10.0)
        transferred_file = workflow.File('transferred.txt', 1000)

        dag_builder = workflow.DagBuilder()
        dag_builder.add_task(before_task)
        dag_builder.add_task(after_task)
        dag_builder.add_file(transferred_file)

        dag_builder.add_edge('before', 'after')
        dag_builder.add_output_file('before', 'transferred.txt')
        dag_builder.add_input_file('after', 'transferred.txt')

        return dag_builder.build()

    def test_should_pass_when_order_is_correct(self):
        dag = self._prepare_parent_child_dag()
        dag.id = '1'

        tasks = {
            ('1', 'parent'): TaskLog(id='parent', started=0.0, finished=10.0, **IRRELEVANT_TASK_ATTRIBUTES),
            ('1', 'child1'): TaskLog(id='child1', started=11.0, finished=13.0, **IRRELEVANT_TASK_ATTRIBUTES),
            ('1', 'child2'): TaskLog(id='child2', started=12.0, finished=15.0, **IRRELEVANT_TASK_ATTRIBUTES),
        }

        result = order_validator.validate(dag, tasks)

        self.assertTrue(result.is_valid)
        self.assertListEqual([], result.errors)

    def test_should_fail_if_any_following_task_was_finished_before(self):
        dag = self._prepare_parent_child_dag()
        dag.id = '1'

        tasks = {
            ('1', 'parent'): TaskLog(id='parent', started=0.0, finished=10.0, **IRRELEVANT_TASK_ATTRIBUTES),
            ('1', 'child1'): TaskLog(id='child1', started=11.0, finished=13.0, **IRRELEVANT_TASK_ATTRIBUTES),
            ('1', 'child2'): TaskLog(id='child2', started=5.0, finished=8.0, **IRRELEVANT_TASK_ATTRIBUTES),
        }

        result = order_validator.validate(dag, tasks)

        self.assertFalse(result.is_valid)
        self.assertIn('child2', result.errors[0])

#    def test_should_fail_if_input_file_was_not_delivered_before_task(self):
#        dag = self._prepare_file_transfer_dag()
#
#        execution_log = order_validator.ExecutionLog()
#        execution_log.add_event(order_validator.TASK_STARTED, 'before', 0.0)
#        execution_log.add_event(order_validator.TASK_FINISHED, 'before', 10.0)
#
#        execution_log.add_event(order_validator.TASK_STARTED, 'after', 20.0)
#        execution_log.add_event(order_validator.TASK_FINISHED, 'after', 30.0)
#
#        result = order_validator.validate(dag, execution_log)







if __name__ == '__main__':
    unittest.main()

