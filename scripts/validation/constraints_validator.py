"""
Validates if experiment constraints were met. Raises error if
  * Total VMs cost exceeded constrained budget.
  * Lifecycle of any VM exceeded constrained deadline.
"""

import math
from log_parser.execution_log import EventType
from validation.common import ValidationResult

SECS_IN_HOUR = 3600.0


def validate(vms, settings):
    def calculate_cost(vm):
        full_hours = math.ceil((vm.finished - vm.started) / SECS_IN_HOUR)
        return full_hours * settings.vm_cost_per_hour

    vms_after_deadline = [vm for vm in vms if vm.finished > settings.deadline]

    deadline_errors = ['VM {} lifecycle ({} - {}) exceeded constrained deadline ({}).'.format(
        vm.id, vm.started, vm.finished, settings.deadline) for vm in vms_after_deadline]

    budget_errors = []
    total_cost = sum([calculate_cost(vm) for vm in vms])
    if total_cost > settings.budget:
        budget_errors.append('Total VMs cost ({}) exceeded budget ({})'.format(total_cost, settings.budget))

    return ValidationResult(deadline_errors + budget_errors)


def validate_experiment(execution_log):
    vms = execution_log.events[EventType.VM]
    settings = execution_log.settings

    return validate(vms, settings)
