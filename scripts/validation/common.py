from collections import namedtuple


class ValidationResult(object):
    def __init__(self, errors):
        self.errors = errors

    @property
    def is_valid(self):
        return len(self.errors) == 0


class Validator(object):
    def validate(self, experiment_log):
        pass


ExperimentSettings = namedtuple('ExperimentSettings', 'deadline budget vm_cost_per_hour')
ExperimentSettingsWithId = namedtuple('ExperimentSettingsWithId', 'id deadline budget vm_cost_per_hour')