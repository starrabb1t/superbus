from superbus import Worker
from pydantic import BaseModel

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "42324232"

class TaskData(BaseModel):
    number_1: int
    number_2: int


class ResponseData(BaseModel):
    number_ans: int


worker = Worker(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

def bad_job(data):
    data = TaskData(**data)

    raise Exception("This exception raised just in purpose of demonstration %)")


# define worker operators dictionary
OPERATORS = {"BAD_JOB": bad_job}

if __name__ == "__main__":
    worker.run(OPERATORS)
