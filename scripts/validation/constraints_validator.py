"""
Validates if experiment constraints were met. Raises error if
  * Total VMs cost exceeded constrained budget.
  * Lifecycle of any VM exceeded constrained deadline.
"""

import math
from common import SimplePricingModel
from common import GooglePricingModel
from log_parser.execution_log import EventType
from validation.common import ValidationResult


def validate(vms, settings):
    vms_after_deadline = [vm for vm in vms if vm.finished > settings.deadline]

    deadline_errors = ['VM {} lifecycle ({} - {}) exceeded constrained deadline ({}).'.format(
        vm.id, vm.started, vm.finished, settings.deadline) for vm in vms_after_deadline]

    budget_errors = []
    total_cost = 0
    if settings.pricing_model == "simple":
        model = SimplePricingModel(settings.billing_time_in_seconds)
        total_cost = sum([model.get_vm_cost_for(vm.price_for_billing_unit, vm.finished - vm.started) for vm in vms])
    elif settings.pricing_model == "google":
        model = GooglePricingModel(settings.billing_time_in_seconds,
                                   settings.first_billing_time_in_seconds)
        total_cost = sum([model.get_vm_cost_for(vm.price_for_billing_unit, vm.finished - vm.started) for vm in vms])
    if total_cost > settings.budget:
        budget_errors.append('Total VMs cost ({}) exceeded budget ({})'.format(total_cost, settings.budget))

    return ValidationResult(deadline_errors + budget_errors)


def validate_experiment(execution_log):
    vms = execution_log.events[EventType.VM]
    settings = execution_log.settings

    return validate(vms, settings)
