import unittest
from log_parser.execution_log import TaskLog
from validation import simulation_validator

def create_job(started, finished, vm):
    return TaskLog(started=started, finished=finished, vm=vm,
                   result='OK', id='parent_1', workflow='1', task_id='parent')


class SimulationValidatorTest(unittest.TestCase):
    def test_should_pass_if_job_does_not_intersects(self):
        jobs = [
            create_job(started=0.0, finished=10.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]

        self.assertTrue(simulation_validator.validate(jobs))

    # def test_should_pass_if_intersecting_jobs_are_on_different_vms

    def test_should_fail_if_jobs_intersects(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]

        self.assertFalse(simulation_validator.validate(jobs))

        # errors

if __name__ == '__main__':
    unittest.main()
