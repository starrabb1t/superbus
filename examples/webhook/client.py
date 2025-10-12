from superbus import Client

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "12345678"
WEBHOOK_URL = "http://0.0.0.0:8080"
BAD_WEBHOOK_URL = "http://bad_webhook"

bus = Client(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

print("1. correct task")
WORKFLOW = ["SUM_TASK"]
bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW, wait_result=False, webhook=WEBHOOK_URL
)

print("2. incorrect task causes validation error")
WORKFLOW = ["SUM_TASK"]
response = bus.pushTask(
    task_data={}, workflow=WORKFLOW, wait_result=True, webhook=WEBHOOK_URL
)
print(response)

print("3. correct task with incorrect webhook_url")
WORKFLOW = ["SUM_TASK"]
response = bus.pushTask(
    task_data={"number_1": 2, "number_2": 3}, workflow=WORKFLOW, wait_result=True, webhook=BAD_WEBHOOK_URL
)
print(response)
