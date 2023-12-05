from superbus import Worker
from pydantic import BaseModel
import time

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "42324232"

class TaskData(BaseModel):
    number_1: int
    number_2: int


class ResponseData(BaseModel):
    number_ans: int


worker = Worker(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

def sum_numbers_job(data):
    data = TaskData(**data)

    # some dummy code
    answer = data.number_1 + data.number_2
    time.sleep(5)

    response = ResponseData(number_ans=answer)
    return response.dict()

TASK_MAP = {"SUM_TASK": sum_numbers_job}

if __name__ == "__main__":
    worker.run(TASK_MAP)

