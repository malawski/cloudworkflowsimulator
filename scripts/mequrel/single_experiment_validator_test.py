import unittest

import single_experiment_validator

class ModuleTest(unittest.TestCase):

    def setUp(self):
        super(ModuleTest, self).setUp()
        pass

    def tearDown(self):
        super(ModuleTest, self).tearDown()
        pass

    def _prepare_task(self):
        task = single_experiment_validator.Task()
        task.task_start = 3.0
        task.computation_start = 4.0
        task.computation_end = 7.0
        task.task_end = 7.5
        return task

    def test_should_pass_when_valid_task(self):
        task = self._prepare_task()

        result = single_experiment_validator.validate_task(task)

        self.assertTrue(result.is_valid)

    def test_should_return_some_message_when_fails(self):
        task = self._prepare_task()
        task.task_start = single_experiment_validator.MISSING_VALUE

        result = single_experiment_validator.validate_task(task)

        self.assertTrue(result.message)


    def test_should_fail_when_task_has_not_started(self):
        task = self._prepare_task()
        task.task_start = single_experiment_validator.MISSING_VALUE

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_fail_when_task_has_not_ended(self):
        task = self._prepare_task()
        task.task_end = single_experiment_validator.MISSING_VALUE

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_fail_when_computation_has_not_started(self):
        task = self._prepare_task()
        task.computation_start = single_experiment_validator.MISSING_VALUE

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_fail_when_computation_has_not_finished(self):
        task = self._prepare_task()
        task.computation_end = single_experiment_validator.MISSING_VALUE

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_hold_task_time_order(self):
        task = self._prepare_task()
        task.task_start = 2.0
        task.task_end = 1.0

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_hold_compute_time_order(self):
        task = self._prepare_task()
        task.computation_start = 5.0
        task.computation_end = 4.0

        result = single_experiment_validator.validate_task(task)

        self.assertFalse(result.is_valid)


if __name__ == '__main__':
    unittest.main()


