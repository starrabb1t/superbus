import time
from superbus import Client

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "42324232"

bus = Client(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

WORKFLOW_1 = ["SUM"]

id = bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW_1, wait_result=False
)["id"]

while True:
    time.sleep(1)
    task = bus.getTask(id)
    print(task)
    if task["status"] == "SUCCESS" or task["status"] == "ERROR":
        break

WORKFLOW_2 = ["MULTIPLY"]
response = bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW_2, wait_result=True
)
print(response)
