import time
import threading

TASK_RUNNING_TIME = 0.1

class Worker:

    def __init__(self):
        self.server = None
        self.client = None
        self.tasks = []
        self._lock = threading.Lock()

    def _work(self):
        while True:
            with self._lock:
                if self.tasks:
                    task = self.tasks.pop()
            time.sleep(TASK_RUNNING_TIME)
            self.finish_work(task)

    def send_task(self):
        pass

    def finish_work(self, task):
        pass

    
