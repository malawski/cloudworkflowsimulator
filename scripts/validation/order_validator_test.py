import unittest
from log_parser.execution_log import TransferLog

from validation import workflow, order_validator
from validation.parsed_log_loader import TaskLog


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

        tasks = [
            TaskLog(id='parent_1', workflow='1', task_id='parent', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='child1_1', workflow='1', task_id='child1', started=11.0, finished=13.0,
                vm=1, result='OK'),
            TaskLog(id='child2_1', workflow='1', task_id='child2', started=12.0, finished=15.0,
                vm=2, result='OK')]

        result = order_validator.validate(dag, tasks)

        self.assertTrue(result.is_valid)
        self.assertListEqual([], result.errors)

    def test_should_fail_if_any_following_task_was_finished_before(self):
        dag = self._prepare_parent_child_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='parent_1', workflow='1', task_id='parent', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='child1_1', workflow='1', task_id='child1', started=11.0, finished=13.0,
                vm=1, result='OK'),
            TaskLog(id='child2_1', workflow='1', task_id='child2', started=5.0, finished=8.0,
                vm=2, result='OK')]

        result = order_validator.validate(dag, tasks)

        self.assertFalse(result.is_valid)
        self.assertIn('child2', result.errors[0])

    def test_should_pass_if_task_was_started_immediately(self):
        dag = self._prepare_parent_child_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='parent_1', workflow='1', task_id='parent', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='child1_1', workflow='1', task_id='child1', started=10.0, finished=13.0,
                vm=1, result='OK'),
            TaskLog(id='child2_1', workflow='1', task_id='child2', started=10.0, finished=15.0,
                vm=2, result='OK')]

        result = order_validator.validate(dag, tasks)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_are_ok(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=1, started=10.0, finished=12.0,
                direction='UPLOAD', file_id='transferred.txt'),
            TransferLog(id='234', job_id='after_1', vm=1, started=12.0, finished=13.0,
                direction='DOWNLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertTrue(result.is_valid)
        self.assertListEqual([], result.errors)

    def test_should_fail_if_file_was_not_downloaded_at_all(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=1, started=10.0, finished=12.0,
                direction='UPLOAD', file_id='transferred.txt'),
        ]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

    def test_should_fail_if_file_was_not_uploaded_at_all(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(
                id='234', job_id='after_1', vm=1, started=12.0,
                finished=13.0, direction='DOWNLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

    def test_should_fail_if_downloaded_to_bad_vm(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=2, started=10.0, finished=12.0,
                direction='UPLOAD', file_id='transferred.txt'),
            TransferLog(id='234', job_id='after_1', vm=1, started=12.0, finished=13.0,
                direction='DOWNLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

    def test_should_fail_if_uploaded_from_bad_vm(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=1, started=10.0, finished=12.0,
                direction='UPLOAD', file_id='transferred.txt'),
            TransferLog(id='234', job_id='after_1', vm=2, started=12.0, finished=13.0,
                direction='DOWNLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

    def test_should_fail_if_upload_instead_of_download(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=1, started=10.0, finished=12.0,
                direction='UPLOAD', file_id='transferred.txt'),
            TransferLog(id='234', job_id='after_1', vm=1, started=12.0, finished=13.0,
                direction='UPLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

    def test_should_fail_if_download_instead_of_upload(self):
        dag = self._prepare_file_transfer_dag()
        dag.id = '1'

        tasks = [
            TaskLog(id='before_1', workflow='1', task_id='before', started=0.0, finished=10.0,
                vm=1, result='OK'),
            TaskLog(id='after_1', workflow='1', task_id='after', started=13.0, finished=18.0,
                vm=1, result='OK')]

        transfers = [
            TransferLog(id='123', job_id='before_1', vm=1, started=10.0, finished=12.0,
                direction='DOWNLOAD', file_id='transferred.txt'),
            TransferLog(id='234', job_id='after_1', vm=1, started=12.0, finished=13.0,
                direction='DOWNLOAD', file_id='transferred.txt')]

        result = order_validator.validate_transfers(dag, tasks, transfers)
        self.assertFalse(result.is_valid)

if __name__ == '__main__':
    unittest.main()

