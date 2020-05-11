class Task:

    def __init__(self, taskid):
        self.taskid = taskid

class Job:

    def __init__(self, jobid, tasks=None):
        self.jobid = jobid
        self.tasks = tasks if tasks else []

