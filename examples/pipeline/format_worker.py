import time
from superbus import Worker
from pydantic import BaseModel

REDIS_HOST = "0.0.0.0"
REDIS_PASSWORD = "42324232"

class FormatWorkerTaskData(BaseModel):
    number_ans: int


class FormatWorkerResponseData(BaseModel):
    text: str


worker = Worker(redis_host=REDIS_HOST, redis_password=REDIS_PASSWORD)


def format_number_job(data):
    data = FormatWorkerTaskData(**data)

    # some dummy code
    answer = f"The answer is '{data.number_ans}'"
    time.sleep(5)

    response = FormatWorkerResponseData(text=answer)
    return response.dict()


# define worker operators dictionary
OPERATORS = {
    "FORMAT": format_number_job,
}

if __name__ == "__main__":
    worker.run(OPERATORS)
