from superbus import Client

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "12345678"

bus = Client(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

WORKFLOW = ["BAD_JOB"]
response = bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW, wait_result=True
)
print(response)
