import unittest

import parse_experiment_log

class MakespannedEventsGluer(unittest.TestCase):
    def test_should_return_empty_list_when_no_events_given(self):
        events = []

        joined_events = parse_experiment_log.glue_fissured_events(events)

        self.assertListEqual([], joined_events)

    def test_should_merge_events_with_same_id(self):
        irrelevant_attributes = { 
            'result': 'OK', 'workflow': 2, 'task_id': 123, 'vm': 32}

        events = [
            parse_experiment_log.TaskLog(
                    started=1000, finished=None, id=1, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=None, finished=2000, id=1, **irrelevant_attributes),
        ]

        expected_events = [
            parse_experiment_log.TaskLog(
                   id=1, started=1000, finished=2000, **irrelevant_attributes)
        ]

        joined_events = parse_experiment_log.glue_fissured_events(events)

        self.assertEqual(set(expected_events), set(joined_events))   

    def test_should_merge_different_params_also(self):
        irrelevant_attributes = { 
                'workflow': 2, 'task_id': 123, 'vm': 32}

        events = [
            parse_experiment_log.TaskLog(
                    started=1000, finished=None, id=1, result=None, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=None, finished=2000, id=1, result='OK', **irrelevant_attributes),
        ]

        expected_events = [
            parse_experiment_log.TaskLog(
                   id=1, started=1000, finished=2000, result='OK', **irrelevant_attributes)
        ]

        joined_events = parse_experiment_log.glue_fissured_events(events)

        self.assertEqual(set(expected_events), set(joined_events)) 

    def test_should_merge_more_events(self):
        irrelevant_attributes = { 
            'result': 'OK', 'workflow': 2, 'task_id': 123, 'vm': 32}

        events = [
            parse_experiment_log.TaskLog(
                    started=1000, finished=None, id=1, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=7833, finished=None, id=2, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=None, finished=2000, id=1, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=None, finished=9500, id=3, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=9000, finished=None, id=3, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                    started=None, finished=8000, id=2, **irrelevant_attributes),
        ]

        expected_events = [
            parse_experiment_log.TaskLog(
                   id=1, started=1000, finished=2000, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                   id=2, started=7833, finished=8000, **irrelevant_attributes),
            parse_experiment_log.TaskLog(
                   id=3, started=9000, finished=9500, **irrelevant_attributes),
        ]

        joined_events = parse_experiment_log.glue_fissured_events(events)

        self.assertEqual(set(expected_events), set(joined_events)) 

if __name__ == '__main__':
    unittest.main()