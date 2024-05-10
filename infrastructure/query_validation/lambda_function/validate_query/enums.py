from enum import Enum


class Task(Enum):
    CREATED = 'Created'
    STOPPED = 'Stopped'
    FINISHED = 'Finished'
    IN_PROGRESS = 'In-progress'
    ERROR = 'Error'


class QueryLog(Enum):
    CREATED = 'Created'
    FAILED = 'Failed'
    CHECKED = 'Checked'
