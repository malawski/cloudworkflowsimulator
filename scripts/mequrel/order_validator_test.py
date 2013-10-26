import unittest

import order_validator
import workflow


class OrderValidatorTest(unittest.TestCase):

    def setUp(self):
        super(OrderValidatorTest, self).setUp()
        pass

    def tearDown(self):
        super(OrderValidatorTest, self).tearDown()
        pass


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

    # before --> (transfered.txt) --> after
    def _prepare_file_transfer_dag(self):
        before_task = workflow.Task('before', 10.0)
        after_task = workflow.Task('after', 10.0)
        transfered_file = workflow.File('transfered.txt', 1000)

        dag_builder = workflow.DagBuilder()
        dag_builder.add_task(before_task)
        dag_builder.add_task(after_task)
        dag_builder.add_file(transfered_file)

        dag_builder.add_edge('before', 'after')
        dag_builder.add_output_file('before', 'transfered.txt')
        dag_builder.add_input_file('after', 'transfered.txt')

        return dag_builder.build()

    def test_should_pass_when_order_is_correct(self):
        dag = self._prepare_parent_child_dag()

        execution_log = order_validator.ExecutionLog()
        execution_log.add_event(order_validator.TASK_STARTED, 'parent', 0.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'parent', 10.0)

        execution_log.add_event(order_validator.TASK_STARTED, 'child1', 11.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'child1', 13.0)

        execution_log.add_event(order_validator.TASK_STARTED, 'child2', 12.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'child2', 15.0)

        result = order_validator.validate(dag, execution_log)

        self.assertTrue(result.is_valid)
        self.assertListEqual([], result.errors)

    def test_should_fail_if_any_following_task_was_finished_before(self):
        dag = self._prepare_parent_child_dag()

        execution_log = order_validator.ExecutionLog()
        execution_log.add_event(order_validator.TASK_STARTED, 'parent', 0.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'parent', 10.0)

        execution_log.add_event(order_validator.TASK_STARTED, 'child1', 11.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'child1', 13.0)

        execution_log.add_event(order_validator.TASK_STARTED, 'child2', 5.0)
        execution_log.add_event(order_validator.TASK_FINISHED, 'child2', 8.0)

        result = order_validator.validate(dag, execution_log)

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

