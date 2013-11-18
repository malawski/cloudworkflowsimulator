"""
Counts overall transfer size that is needed to run given workflow and total sum of file sizes.

Example of usage:
  $ python -m experiment_analysis.count_storage /some/path/to/checked.dag

"""

import argparse
from validation import dag_loader


def main():
    args = parse_arguments()
    filename = args.filename
    dag = load_dag(filename)

    total_file_size = get_total_file_size(dag)
    total_transfer_size = get_total_transfer_size(dag)

    main_file_name = filename.split('/')[-1]

    print('Filename: {}'.format(main_file_name))
    print('Total sum of transfer sizes in GBs: {:.2f}'.format(to_gbs(total_transfer_size)))
    print('Total sum of file sizes in GBs: {:.2f}'.format(to_gbs(total_file_size)))


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='Path to *.dag filename with workflow definition')
    return parser.parse_args()


def load_dag(filename):
    dag_file = open(filename, 'r')
    dag = dag_loader.parse_dag(dag_file.read())
    dag.id = 1
    dag_file.close()
    return dag


def get_total_file_size(dag):
    return sum(file.size for file in dag.files)


def get_total_transfer_size(dag):
    files_by_id = {file.filename: file for file in dag.files}

    def get_total_transfer_for_task(task):
        return sum(files_by_id[file_id].size for file_id in (task.files_needed + task.files_produced))

    return sum(get_total_transfer_for_task(task) for task in dag.tasks)


BYTES_IN_GB = 1024 ** 3


def to_gbs(file_size):
    return float(file_size) / BYTES_IN_GB


if __name__ == "__main__":
    main()