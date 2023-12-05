import time
from datetime import datetime
import httpx
import traceback
from .utils import *
import redis


class StatusUpdater:
    def __init__(self, r: redis.Redis):
        self._redis = r

    def get_task_by_id(self, task_id: str, with_data: bool = False) -> TaskModel:

        task = self._redis.hget("tasks", task_id)
        task = TaskModel(**json.loads(task))

        if task:
            if with_data:
                task.data = self.get_task_data_by_id(task_id)
            
            return task
        
    def get_task_data_by_id(self, task_id: str) -> Dict:
        data = self._redis.hget("task_data", task_id)
        if data:
            return json.loads(data)
        else:
            return {}

    def set_created(self, task : TaskModel) -> None:

        task.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT_STR)
        task.status = "CREATED"
        self._redis.hset("tasks", task.id, json.dumps(task.dict()))
        keydb_expiremember(self._redis, "tasks", task.id)

    def set_in_progress(self, task: TaskModel) -> None:

        task.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT_STR)
        task.status = "IN PROGRESS"
        self._redis.hset("tasks", task.id, json.dumps(task.dict()))
        keydb_expiremember(self._redis, "tasks", task.id)

    def set_success(self, task: TaskModel) -> None:
        
        task.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT_STR)
        task.status = "SUCCESS"
        self._redis.hset("tasks", task.id, json.dumps(task.dict()))
        keydb_expiremember(self._redis, "tasks", task.id)

    def set_error(self, error_message: str, task: TaskModel) -> None:

        task.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT_STR)
        task.status = "ERROR"
        task.error = error_message
        self._redis.hset("tasks", task.id, json.dumps(task.dict()))
        keydb_expiremember(self._redis, "tasks", task.id)

    def get_timeout(self, task: TaskModel) -> TaskModel:

        task.timestamp = datetime.now().strftime(TIMESTAMP_FORMAT_STR)
        task.status = "TIMEOUT"

        return task

    def wait_until_complete(self, task_id: str, timeout: int, polling_period: int) -> TaskModel:
        
        time_start = time.time()

        while True:

            result = self.get_task_by_id(task_id, with_data=True)

            if result.status == "ERROR" or result.status == "SUCCESS":

                return result

            time.sleep(polling_period)

            time_passed = time.time() - time_start

            if time_passed > timeout:
                logger.warning("timeout!")
                return self.get_timeout(result)

    def send_webhook_post(self, task : TaskModel, url : str) -> None:
        try:
            r = httpx.post(url, json=task.json())
            logger.info(
                f"post to webhook {url} for task '{task.id}' completed with status {r.status_code}"
            )
        except:
            logger.error(
                f"post to webhook {url} failed! Traceback: {traceback.format_exc()}"
            )
