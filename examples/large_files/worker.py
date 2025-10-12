import time
from superbus import Worker
from pydantic import BaseModel
import random

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "12345678"


class LFTaskData(BaseModel):
    large_data: str


class LFResponseData(BaseModel):
    large_data: str


worker = Worker(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)

def process_large_data(data):
    data = LFTaskData(**data)

    #large_data_size = len(data.large_data)
    time.sleep(2)
    large_data = str(random.randint(0,9)) * 20000000

    response = LFResponseData(large_data=large_data)
    return response.dict()


# define worker operators dictionary
OPERATORS = {
    "LFPROC" : process_large_data
}

if __name__ == "__main__":
    worker.run(OPERATORS)
