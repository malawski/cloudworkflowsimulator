'''Simple generic log parser implementation.'''

from collections import namedtuple
import re

Pattern = namedtuple('LogPattern', 'regex type set_values')


class LogParser(object):
    def __init__(self):
        self.patterns = []

    def add_pattern(self, pattern):
        self.patterns.append(pattern)

    def match_line(self, line):
        for pattern in self.patterns:
            matched = re.match(pattern.regex, line.strip())
            if matched:
                groups = matched.groupdict()
                values = dict(pattern.set_values.items() + groups.items())
                return pattern.type(**values)

        return None

    def parse(self, filename):
        with open(filename, "r") as in_file:
            log_objects = [self.match_line(line) for line in in_file]
            log_objects = [log_object for log_object in log_objects if log_object]

        return log_objects

