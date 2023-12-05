import uuid
from .updater import *

WAIT_TIMEOUT_SEC = 300
WAIT_POLLING_PERIOD_SEC = 1

class Client:
    def __init__(
        self,
        redis_host,
        redis_port=DEFAULT_REDIS_PORT,
        redis_password = None
    ):

        if redis_password:
            self._redis = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        else:
            self._redis = redis.Redis(host=redis_host, port=redis_port)

        self.updater = StatusUpdater(self._redis)

    def getTask(self, task_id: str) -> Dict:

        task = self.updater.get_task_by_id(task_id)    

        if task.status == "ERROR" or task.status == "SUCCESS":
            data = self.updater.get_task_data_by_id(task_id)

            task = task.dict()
            task["data"] = data
            return task
              
        return task.dict()

    def pushTask(
        self,
        task_data: Dict,
        workflow: List,
        wait_result=False,
        wait_timeout = WAIT_TIMEOUT_SEC,
        wait_polling = WAIT_POLLING_PERIOD_SEC,
        webhook=None,
    ) -> Dict:
        
        try:

            task = TaskModel(
                id=uuid.uuid4().hex,
                workflow=workflow
            )

            if webhook:
                task.webhook = webhook

            task_data_json =  json.dumps(task_data)
            self._redis.hset("task_data", task.id, task_data_json)
            keydb_expiremember(self._redis, "task_data", task.id)
            
            self.updater.set_created(task)
            self._redis.lpush(workflow[0], task.id)
            
            logger.info("task created")

            if wait_result:
                result = self.updater.wait_until_complete(task.id, wait_timeout, wait_polling)
                logger.info(f"return response")
                return result
            else:
                result = self.updater.get_task_by_id(task.id)
                logger.info(f"return id")
                return result.dict()
            
        except:
            logger.error(traceback.format_exc())
