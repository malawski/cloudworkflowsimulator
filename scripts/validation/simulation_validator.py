from validation.order_validator import ValidationResult

ENDS, STARTS = range(2)


def generate_job_events_sequentially(jobs):
    events = []

    for job in jobs:
        events.append((job.started, STARTS, job))
        events.append((job.finished, ENDS, job))

    return sorted(events)


def get_intersections(started_jobs, current_job):
    return [(current_job, started_job) for started_job in started_jobs]


def get_intersecting_jobs(jobs):
    intersecting_jobs = []

    started_jobs = set()
    for (_, event_type, job) in generate_job_events_sequentially(jobs):
        if event_type == STARTS:
            intersections = get_intersections(started_jobs, job)
            intersecting_jobs.extend(intersections)
            started_jobs.add(job)
        else:
            started_jobs.remove(job)

    return intersecting_jobs

def validate(jobs):
    intersecting_jobs = get_intersecting_jobs(jobs)

    errors = ['Job {} was executed in the same time as job {} on the same VM'.format(job1.id, job2.id)
              for (job1, job2) in intersecting_jobs]

    return ValidationResult(errors)