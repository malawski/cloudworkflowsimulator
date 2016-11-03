from collections import namedtuple

import math


class ValidationResult(object):
    def __init__(self, errors):
        self.errors = errors

    @property
    def is_valid(self):
        return len(self.errors) == 0


class Validator(object):
    def validate(self, experiment_log):
        pass


class PricingModel(object):
    def __init__(self, billing_time_in_seconds):
        self.billing_time_in_seconds = billing_time_in_seconds


class SimplePricingModel(PricingModel):
    def get_vm_cost_for(self, price_for_billing_unit, runtime_in_seconds):
        billing_units = runtime_in_seconds / self.billing_time_in_seconds
        full_billing_units = math.ceil(billing_units)
        return full_billing_units * float(price_for_billing_unit)


class GooglePricingModel(PricingModel):
    def __init__(self, billing_time_in_seconds, first_billing_time_in_seconds):
        self.billing_time_in_seconds = billing_time_in_seconds
        self.first_billing_time_in_seconds = first_billing_time_in_seconds

    def get_vm_cost_for(self, price_for_billing_unit, runtime_in_seconds):
        total_vm_cost = 0
        if runtime_in_seconds > 0:
            total_vm_cost += self.first_billing_time_in_seconds * price_for_billing_unit / self.billing_time_in_seconds
        if runtime_in_seconds > self.first_billing_time_in_seconds:
            runtime_in_seconds -= self.first_billing_time_in_seconds
            billing_units = runtime_in_seconds / self.billing_time_in_seconds
            full_billing_units = math.ceil(billing_units)
            total_vm_cost += full_billing_units * price_for_billing_unit
        return total_vm_cost


ExperimentSettings = namedtuple('ExperimentSettings',
                                'deadline budget pricing_model billing_time_in_seconds first_billing_time_in_seconds')
ExperimentSettingsWithId = namedtuple('ExperimentSettingsWithId',
                                      'id deadline budget pricing_model billing_time_in_seconds first_billing_time_in_seconds')
