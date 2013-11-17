import argparse

from validation import constraints_validator
from validation import order_validator
from validation import parsed_log_loader
from validation import single_task_validator
from validation import simulation_validator

ALL_VALIDATORS = 'all'

VALIDATORS = {
    'single_task': single_task_validator.validate_transfer,
    'order': order_validator.validate,
    'simulation': simulation_validator.validate_experiment,
    'constraints': constraints_validator.validate_experiment,
}


class ExperimentValidatorError(Exception):
    pass


def main():
    args = parse_arguments()
    execution_log = load_experiment_log(args.filename)
    validators = get_validators(args.validator)

    validation_errors = validate_with(validators, execution_log)

    print_errors(validation_errors)


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