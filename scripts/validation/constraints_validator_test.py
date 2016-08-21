import unittest
from log_parser.execution_log import VMLog
from validation.common import ExperimentSettings

import constraints_validator

DEFAULT_DEADLINE = 10000.0
DEFAULT_BUDGET = 300.0
DEFAULT_VM_COST = 10.0

SECS_IN_HOUR = 3600.0

IRRELEVANT_VM_ATTRIBUTES = {
    'cores': 1
}


class ConstraintsValidatorTest(unittest.TestCase):
    def test_should_pass_when_vms_terminated_within_deadline(self):
        vms = [
            VMLog(started=0.0, finished=13.0, id=1, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=14.0, finished=19.0, id=2, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, vm_cost_per_hour=DEFAULT_VM_COST)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_pass_when_vms_terminated_equally_with_deadline(self):
        vms = [
            VMLog(started=0.0, finished=13.0, id=1, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=14.0, finished=20.0, id=2, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, vm_cost_per_hour=DEFAULT_VM_COST)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_fail_when_vms_terminated_after_deadline(self):
        vms = [
            VMLog(started=14.0, finished=22.0, id=1, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, vm_cost_per_hour=DEFAULT_VM_COST)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)
        self.assertLessEqual(1, len(result.errors))

    def test_should_pass_when_vms_cost_was_within_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1234.0, vm_cost_per_hour=1000.0, deadline=DEFAULT_DEADLINE)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

    def test_should_pass_when_vms_cost_was_equal_to_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, vm_cost_per_hour=1000.0, deadline=DEFAULT_DEADLINE)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_vms_cost_exceeded_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=333.0, vm_cost_per_hour=1000.0, deadline=DEFAULT_DEADLINE)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

    def test_should_count_full_hours_only(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR + 1.0, id=1, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, vm_cost_per_hour=1000.0, deadline=DEFAULT_DEADLINE)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

    def test_should_sum_vm_hours(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=2, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=3, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=2999.0, vm_cost_per_hour=1000.0, deadline=DEFAULT_DEADLINE)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)


if __name__ == '__main__':
    unittest.main()
