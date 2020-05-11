import config
from job import Job, Task
from producer import ProducerClient
from consumer import ConsumerClient

def main_producer():
    
    job = Job(config.JOB_ID)
    for i in range(config.TASK_NUM):
        job.tasks.append(Task(i))
    
    p = ProducerClient()
    p.submit_job(job)


def main_consumer():
    consumer = ConsumerClient()
    consumer.consume([config.JOB_SUBMIT_TOPIC + config.JOB_ID])


if __name__ == "__main__":
    import sys
    main_type = sys.argv[1]
    if main_type == "p":
        main_producer()
    else:
        main_consumer()
