from superbus import Client

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "42324232"
WEBHOOK_URL = "http://0.0.0.0:8080"

bus = Client(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

# correct task
WORKFLOW = ["SUM_TASK"]
bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW, wait_result=False, webhook=WEBHOOK_URL
)

# incorrect task causes validation error
WORKFLOW = ["SUM_TASK"]
response = bus.pushTask(
    task_data={}, workflow=WORKFLOW, wait_result=True, webhook=WEBHOOK_URL
)
print(response)

