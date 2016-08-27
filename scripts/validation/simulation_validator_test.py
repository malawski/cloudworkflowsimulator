import unittest
from log_parser.execution_log import TaskLog, TransferLog, VMLog
from validation import simulation_validator


def create_job(started, finished, vm):
    return TaskLog(started=started, finished=finished, vm=vm,
        result='OK', id='parent_1', workflow='1', task_id='parent')


def create_transfer(started, finished, vm):
    return TransferLog(started=started, finished=finished, vm=vm,
        id='123', job_id='before_1', direction='UPLOAD', file_id='transferred.txt')


def create_vm(started, finished, id, cores):
    return VMLog(started=started, finished=finished, id=id, cores=cores)


class SimulationValidatorTest(unittest.TestCase):
    def test_should_pass_if_job_does_not_intersects(self):
        jobs = [
            create_job(started=0.0, finished=10.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=13.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_pass_if_jobs_are_contiguous(self):
        jobs = [
            create_job(started=0.0, finished=10.0, vm=1),
            create_job(started=10.0, finished=13.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=13.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_fail_if_jobs_intersects_on_the_same_single_core_vm(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=13.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertFalse(result.is_valid)
        self.assertLessEqual(1, len(result.errors))

    def test_should_pass_if_jobs_intersect_on_the_same_dual_core_vm(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=13.0, id=1, cores=2)]

        result = simulation_validator.validate(jobs, jobs, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_intersecting_jobs_are_on_different_vms(self):
        jobs = [
            create_job(started=0.0, finished=12.0, vm=1),
            create_job(started=11.0, finished=13.0, vm=2)]
        vms = [
            create_vm(started=0.0, finished=13.0, id=1, cores=1),
            create_vm(started=11.0, finished=13.0, id=2, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_not_intersects_computation(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]
        transfers = [
            create_transfer(started=6.0, finished=12.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=12.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_are_contiguous_to_computation(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]
        transfers = [
            create_transfer(started=5.0, finished=12.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=12.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_are_immediate(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1),
            create_job(started=5.0, finished=12.0, vm=1)]
        transfers = [
            create_transfer(started=5.0, finished=5.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=12.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_transfer_intersects_computation_on_the_same_single_core_vm(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]
        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=9.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertFalse(result.is_valid)

    def test_should_pass_if_transfer_intersects_computation_on_the_same_dual_core_vm(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]
        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=9.0, id=1, cores=2)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfers_intersect_on_the_same_dual_core_vm(self):
        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=1),
            create_transfer(started=5.0, finished=7.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=9.0, id=1, cores=2)]

        result = simulation_validator.validate([], transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_transfer_intersects_computation_on_different_vms(self):
        jobs = [
            create_job(started=0.0, finished=5.0, vm=1)]
        transfers = [
            create_transfer(started=3.0, finished=9.0, vm=2)]
        vms = [
            create_vm(started=0.0, finished=10.0, id=1, cores=1),
            create_vm(started=0.0, finished=10.0, id=2, cores=1)]

        result = simulation_validator.validate(jobs, transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_pass_if_job_was_done_within_vm_lifecycle(self):
        jobs = [
            create_job(started=3.0, finished=5.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=5.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_job_was_done_on_non_existing_vm(self):
        jobs = [
            create_job(started=3.0, finished=5.0, vm=1)]
        vms = []

        result = simulation_validator.validate(jobs, [], vms)

        self.assertFalse(result.is_valid)

    def test_should_fail_if_job_was_done_out_of_vm_lifecycle(self):
        jobs = [
            create_job(started=3.0, finished=5.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=4.0, id=1, cores=1)]

        result = simulation_validator.validate(jobs, [], vms)

        self.assertFalse(result.is_valid)

    def test_should_pass_if_transfer_was_done_within_vm_lifecycle(self):
        transfers = [
            create_transfer(started=3.0, finished=5.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=5.0, id=1, cores=1)]

        result = simulation_validator.validate([], transfers, vms)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_transfer_was_done_on_non_existing_vm(self):
        transfers = [
            create_transfer(started=3.0, finished=5.0, vm=1)]
        vms = []

        result = simulation_validator.validate([], transfers, vms)

        self.assertFalse(result.is_valid)

    def test_should_fail_if_transfer_was_done_out_of_vm_lifecycle(self):
        transfers = [
            create_transfer(started=3.0, finished=5.0, vm=1)]
        vms = [
            create_vm(started=0.0, finished=4.0, id=1, cores=1)]

        result = simulation_validator.validate([], transfers, vms)

        self.assertFalse(result.is_valid)


if __name__ == '__main__':
    unittest.main()
