import time
from superbus import Client
import random

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "12345678"

bus = Client(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

WORKFLOW = ["LFPROC"]

large_data = str(random.randint(0,9)) * 20000000

task = bus.pushTask(
    task_data={"large_data" : large_data}, workflow=WORKFLOW, wait_result=True
)

print(task["status"])