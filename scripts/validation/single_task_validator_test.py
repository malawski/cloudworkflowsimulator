import unittest
from validation import single_task_validator
from validation.parsed_log_loader import TaskLog
from validation.parsed_log_loader import TransferLog
from validation.parsed_log_loader import VMLog

IRRELEVANT_TASK_ATTRIBUTES = {
    'id': 'some_id',
    'workflow': 'some_workflow',
    'task_id': 'some_task_id',
    'vm': 1,
    'result': 'OK'
}

IRRELEVANT_TRANSFER_ATTRIBUTES = {
    'id': 'some_id',
    'vm': 1,
    'direction': 'UPLOAD',
    'job_id': 23,
    'file_id': 'file.txt',
}

IRRELEVANT_VM_ATTRIBUTES = {
    'id': 'some_id',
    'price_for_billing_unit': 1.,
    'cores': 1
}


class SingleTaskValidatorTest(unittest.TestCase):
    def test_should_pass_when_valid_task(self):
        task = TaskLog(started=3.0, finished=5.0, **IRRELEVANT_TASK_ATTRIBUTES)

        result = single_task_validator.validate_task(task)

        self.assertTrue(result.is_valid)

    def test_should_return_some_message_when_fails(self):
        task = TaskLog(started=single_task_validator.MISSING_VALUE,
                       finished=single_task_validator.MISSING_VALUE,
                       **IRRELEVANT_TASK_ATTRIBUTES)

        result = single_task_validator.validate_task(task)
        self.assertTrue(result.message)

    def test_should_fail_when_task_has_not_started(self):
        task = TaskLog(started=single_task_validator.MISSING_VALUE,
                       finished=5.0, **IRRELEVANT_TASK_ATTRIBUTES)

        result = single_task_validator.validate_task(task)
        self.assertFalse(result.is_valid)

    def test_should_fail_when_task_has_not_ended(self):
        task = TaskLog(finished=single_task_validator.MISSING_VALUE,
                       started=5.0, **IRRELEVANT_TASK_ATTRIBUTES)

        result = single_task_validator.validate_task(task)
        self.assertFalse(result.is_valid)

    def test_should_hold_task_time_order(self):
        task = TaskLog(started=5.0, finished=3.0, **IRRELEVANT_TASK_ATTRIBUTES)

        result = single_task_validator.validate_task(task)

        self.assertFalse(result.is_valid)

    def test_should_pass_when_valid_transfer(self):
        task = TransferLog(started=3.0, finished=5.0, **IRRELEVANT_TRANSFER_ATTRIBUTES)

        result = single_task_validator.validate_transfer(task)

        self.assertTrue(result.is_valid)

    def test_should_return_some_message_when_transfer_validation_fails(self):
        task = TransferLog(started=single_task_validator.MISSING_VALUE,
                           finished=single_task_validator.MISSING_VALUE,
                           **IRRELEVANT_TRANSFER_ATTRIBUTES)

        result = single_task_validator.validate_transfer(task)
        self.assertTrue(result.message)

    def test_should_fail_when_transfer_has_not_started(self):
        task = TransferLog(started=single_task_validator.MISSING_VALUE,
                           finished=5.0, **IRRELEVANT_TRANSFER_ATTRIBUTES)

        result = single_task_validator.validate_transfer(task)
        self.assertFalse(result.is_valid)

    def test_should_fail_when_transfer_has_not_ended(self):
        task = TransferLog(finished=single_task_validator.MISSING_VALUE,
                           started=5.0, **IRRELEVANT_TRANSFER_ATTRIBUTES)

        result = single_task_validator.validate_transfer(task)
        self.assertFalse(result.is_valid)

    def test_should_hold_transfer_time_order(self):
        task = TransferLog(started=5.0, finished=3.0, **IRRELEVANT_TRANSFER_ATTRIBUTES)

        result = single_task_validator.validate_transfer(task)

        self.assertFalse(result.is_valid)

    def test_should_pass_when_valid_vm(self):
        task = VMLog(started=3.0, finished=5.0, **IRRELEVANT_VM_ATTRIBUTES)

        result = single_task_validator.validate_vm(task)

        self.assertTrue(result.is_valid)

    def test_should_return_some_message_when_vm_validation_fails(self):
        task = VMLog(started=single_task_validator.MISSING_VALUE,
                     finished=single_task_validator.MISSING_VALUE,
                     **IRRELEVANT_VM_ATTRIBUTES)

        result = single_task_validator.validate_vm(task)
        self.assertTrue(result.message)

    def test_should_fail_when_vm_has_not_started(self):
        task = VMLog(started=single_task_validator.MISSING_VALUE,
                     finished=5.0, **IRRELEVANT_VM_ATTRIBUTES)

        result = single_task_validator.validate_vm(task)
        self.assertFalse(result.is_valid)

    def test_should_fail_when_vm_has_not_ended(self):
        task = VMLog(finished=single_task_validator.MISSING_VALUE,
                     started=5.0, **IRRELEVANT_VM_ATTRIBUTES)

        result = single_task_validator.validate_vm(task)
        self.assertFalse(result.is_valid)

    def test_should_hold_vm_time_order(self):
        task = VMLog(started=5.0, finished=3.0, **IRRELEVANT_VM_ATTRIBUTES)

        result = single_task_validator.validate_vm(task)

        self.assertFalse(result.is_valid)


if __name__ == '__main__':
    unittest.main()
