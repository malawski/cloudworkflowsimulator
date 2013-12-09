"""
A script that defines common validator interface and enables validators to be run on experiment logs.

Can run any validator that is enclosed in VALIDATORS dict and have a method:
  validate_experiment(execution_log)
That gets an ExecutionLog object and return ValidationResult.

Example of usage:
  $ python -m validation.experiment_validator preprocessed.log
  $ python -m validation.experiment_validator preprocessed.log --validator all
  $ python -m validation.experiment_validator preprocessed.log --validator order

First two commands are equivalent. They fire all accessible validators.
"""

import argparse
import sys

from validation import constraints_validator
from validation import order_validator
from validation import parsed_log_loader
from validation import single_task_validator
from validation import simulation_validator

ALL_VALIDATORS = 'all'

VALIDATORS = {
    'single_task': single_task_validator.validate_experiment,
    'order': order_validator.validate_experiment,
    'simulation': simulation_validator.validate_experiment,
    'constraints': constraints_validator.validate_experiment,
}


class ExperimentValidatorError(Exception):
    pass


def get_return_code(validation_errors):
    return 0 if not validation_errors else 1


def main():
    args = parse_arguments()
    execution_log = load_experiment_log(args.filename)
    validators = get_validators(args.validator)

    validation_errors = validate_with(validators, execution_log)
    print_errors(validation_errors)
    sys.exit(get_return_code(validation_errors))


def parse_arguments():
    validator_choices = [ALL_VALIDATORS] + VALIDATORS.keys()
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--validator', choices=validator_choices, default=ALL_VALIDATORS)
    parser.add_argument('filename')
    args = parser.parse_args()
    return args


def load_experiment_log(filename):
    infile = open(filename, 'r')
    execution_log = parsed_log_loader.read_log(infile.read())
    infile.close()
    return execution_log


def get_validators(validation_mode):
    if validation_mode == ALL_VALIDATORS:
        return VALIDATORS.values()

    if validation_mode in VALIDATORS:
        return [VALIDATORS[validation_mode]]

    raise ExperimentValidatorError('There is no validator "{}"'.format(validation_mode))


def validate_with(validators, execution_log):
    validation_errors = []
    for validator in validators:
        result = validator(execution_log)
        validation_errors.extend(result.errors)
    return validation_errors


def print_errors(validation_errors):
    for error in validation_errors:
        print(error)


if __name__ == '__main__':
    main()