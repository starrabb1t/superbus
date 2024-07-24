import time
from datetime import datetime
import httpx
from .utils import *
import redis

class StatusUpdater:
    def __init__(self, r: redis.Redis):
        self._redis = r

    def get_task_by_id(self, task_id: str) -> TaskModel:

        task = self._redis.hget("tasks", task_id)
        task = TaskModel(**json.loads(task))

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

        return task.dict()

    def wait_until_complete(self, task_id: str, timeout: int, polling_period: int) -> Dict:
        
        time_start = time.time()

        while True:

            task = self.get_task_by_id(task_id)

            if task.status == "ERROR" or task.status == "SUCCESS":
                
                data = self.get_task_data_by_id(task_id)

                task = task.dict()
                task["data"] = data

                return task

            time.sleep(polling_period)

            time_passed = time.time() - time_start

            if time_passed > timeout:
                logger.warning("timeout!")
                return self.get_timeout(task)

    def send_webhook_post(self, task_dict : Dict, url : str) -> None:
        
        task_id = task_dict["id"]
        task_json = json.dumps(task_dict)

        try:
            r = httpx.post(url, json=task_json, timeout=DEFAULT_WEBHOOK_TIMEOUT_SEC)
            logger.info(
                f"post to webhook {url} for task '{task_id}' completed with status {r.status_code}"
            )
        except httpx.TimeoutException:
            logger.warning(traceback.format_exc())

