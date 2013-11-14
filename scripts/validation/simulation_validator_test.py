import unittest
from log_parser.execution_log import TaskLog, TransferLog
from validation import simulation_validator

def create_job(started, finished, vm):
    return TaskLog(started=started, finished=finished, vm=vm,
                   result='OK', id='parent_1', workflow='1', task_id='parent')


def create_transfer(started, finished, vm):
    return TransferLog(started=started, finished=finished, vm=vm,
                       id='123', job_id='before_1', direction='UPLOAD', file_id='transferred.txt')


class SimulationValidatorTest(unittest.TestCase):
    def test_should_pass_if_job_does_not_intersects(self):
        jobs = [
            create_job(started=0.0, finished=10.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]

        result = simulation_validator.validate(jobs, [])

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_fail_if_jobs_intersects_on_the_same_vm(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]

        result = simulation_validator.validate(jobs, [])

        self.assertFalse(result.is_valid)
        self.assertEqual(1, len(result.errors))

    def test_should_pass_if_intersecting_jobs_are_on_different_vms(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=2)]

        result = simulation_validator.validate(jobs, [])

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_not_intersects_computation(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]

        transfers = [
            create_transfer(started=6.0, finished=12.0, vm=1)]

        result = simulation_validator.validate(jobs, transfers)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_transfer_intersects_computation_on_the_same_vm(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]

        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=1)]

        result = simulation_validator.validate(jobs, transfers)

    def test_should_pass_if_transfer_intersects_computation_on_different_vms(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]

        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=2)]

        result = simulation_validator.validate(jobs, transfers)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfer_intersects_computation_on_different_vms(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]

        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=2)]

        result = simulation_validator.validate(jobs, transfers)



if __name__ == '__main__':
    unittest.main()
