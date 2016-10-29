import unittest
from log_parser.execution_log import VMLog
from validation.common import ExperimentSettings

import constraints_validator

DEFAULT_DEADLINE = 10000.0
DEFAULT_BUDGET = 300.0
DEFAULT_VM_COST = 10.0
SECS_IN_HOUR = 3600

SIMPLE_PRICING_MODEL = "simple"
GOOGLE_PRICING_MODEL = "google"

IRRELEVANT_VM_ATTRIBUTES = {
    'cores': 1
}


class ConstraintsValidatorTest(unittest.TestCase):
    def test_should_pass_when_vms_terminated_within_deadline(self):
        vms = [
            VMLog(started=0.0, finished=13.0, id=1, price_for_billing_unit=DEFAULT_VM_COST, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=14.0, finished=19.0, id=2, price_for_billing_unit=DEFAULT_VM_COST,
                  **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_pass_when_vms_terminated_equally_with_deadline(self):
        vms = [
            VMLog(started=0.0, finished=13.0, id=1, price_for_billing_unit=DEFAULT_VM_COST, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=14.0, finished=20.0, id=2, price_for_billing_unit=DEFAULT_VM_COST,
                  **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)
        self.assertEqual([], result.errors)

    def test_should_fail_when_vms_terminated_after_deadline(self):
        vms = [
            VMLog(started=14.0, finished=22.0, id=1, price_for_billing_unit=DEFAULT_VM_COST,
                  **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(deadline=20.0, budget=DEFAULT_BUDGET, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)
        self.assertLessEqual(1, len(result.errors))

    def test_should_pass_when_vms_cost_was_within_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1234.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=20.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1234.0, deadline=DEFAULT_DEADLINE, pricing_model=GOOGLE_PRICING_MODEL,
                                      billing_time_in_seconds=60, first_billing_time_in_seconds=600)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

    def test_should_pass_when_vms_cost_was_equal_to_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=100.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, deadline=DEFAULT_DEADLINE, pricing_model=GOOGLE_PRICING_MODEL,
                                      billing_time_in_seconds=360, first_billing_time_in_seconds=1800)

        result = constraints_validator.validate(vms, settings)

        self.assertTrue(result.is_valid)

    def test_should_fail_if_vms_cost_exceeded_budget(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=333.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

        # first billing time is enough to exeed budget
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=100.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=333.0, deadline=DEFAULT_DEADLINE, pricing_model=GOOGLE_PRICING_MODEL,
                                      billing_time_in_seconds=360, first_billing_time_in_seconds=1800)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

        # budget is exeeded after first_billing_time
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=40.0, **IRRELEVANT_VM_ATTRIBUTES)]

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

    def test_should_count_full_billing_units_only(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR + 1.0, id=1, price_for_billing_unit=1000.0,
                  **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR + 1.0, id=1, price_for_billing_unit=10.0,
                  **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=1000.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR / 100,
                                      first_billing_time_in_seconds=SECS_IN_HOUR / 10)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

    def test_should_sum_vm_billing_units(self):
        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=2, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=3, price_for_billing_unit=1000.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=2999.0, deadline=DEFAULT_DEADLINE, pricing_model=SIMPLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR, first_billing_time_in_seconds=None)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)

        vms = [
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=1, price_for_billing_unit=10.0, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=2, price_for_billing_unit=10.0, **IRRELEVANT_VM_ATTRIBUTES),
            VMLog(started=0.0, finished=SECS_IN_HOUR, id=3, price_for_billing_unit=10.0, **IRRELEVANT_VM_ATTRIBUTES)]

        settings = ExperimentSettings(budget=2999.0, deadline=DEFAULT_DEADLINE, pricing_model=GOOGLE_PRICING_MODEL,
                                      billing_time_in_seconds=SECS_IN_HOUR / 100,
                                      first_billing_time_in_seconds=SECS_IN_HOUR / 10)

        result = constraints_validator.validate(vms, settings)

        self.assertFalse(result.is_valid)


if __name__ == '__main__':
    unittest.main()
