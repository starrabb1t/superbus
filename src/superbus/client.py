import uuid
from .updater import *

class Client:
    def __init__(
        self,
        redis_host,
        redis_port=DEFAULT_REDIS_PORT,
        redis_password=None,
        logical_db=0,
        expire_delay=None
    ):

        if redis_password:
            self._redis = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                password=redis_password, 
                db=logical_db,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_SEC,
                socket_timeout=REDIS_CONNECTION_TIMEOUT_SEC, 
                socket_keepalive=True,
                socket_connect_timeout=REDIS_CONNECTION_TIMEOUT_SEC, 
                retry_on_timeout=True
            )
        else:
            self._redis = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                db=logical_db,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_SEC,
                socket_timeout=REDIS_CONNECTION_TIMEOUT_SEC, 
                socket_keepalive=True,
                socket_connect_timeout=REDIS_CONNECTION_TIMEOUT_SEC, 
                retry_on_timeout=True
            )

        self.expire_delay = expire_delay
        self.updater = StatusUpdater(self._redis)

    def getTask(self, task_id: str) -> Dict:

        task = self.updater.get_task_by_id(task_id)    

        if task.status == "ERROR" or task.status == "SUCCESS":
            data = self.updater.get_task_data_by_id(task_id)

            task = task.dict()
            task["data"] = data
            return task
              
        return task.dict()

    def listTasks(self) -> List[str]:
        """
        Возвращает список всех Task ID, присутствующих в Redis.
        """
        keys = self._redis.hkeys("tasks")
        return [k.decode() for k in keys]

    def clearCompleted(self) -> int:
        """
        Удаляет завершённые задачи (SUCCESS/ERROR) и "висячие" task_data-записи,
        у которых нет对应 записи в tasks.
        Возвращает количество удалённых элементов.
        """
        removed = 0
        tasks = self._redis.hgetall("tasks")
        task_data = self._redis.hkeys("task_data")
        valid_ids = set(tasks.keys())

        # 1️⃣ Чистим завершённые задачи
        for tid, task_json in tasks.items():
            task = json.loads(task_json)
            if task.get("status") in ("SUCCESS", "ERROR"):
                self._redis.hdel("tasks", tid)
                self._redis.hdel("task_data", tid)
                removed += 1

        # 2️⃣ Удаляем "висячие" task_data
        for tid in task_data:
            if tid not in valid_ids:
                self._redis.hdel("task_data", tid)
                removed += 1

        logger.info(f"Removed {removed} obsolete tasks (completed + orphaned)")
        return removed


    def getQueue(self, workflow: List[str] = None):

        queue = {}

        if workflow:
            for node in workflow:
                queue[node] = self._redis.llen(node)
        else:
            for node in self._redis.smembers("REGISTERED_OPERATORS"):
                queue[node.decode()] = self._redis.llen(node)

        return queue

    def pushTask(
        self,
        task_data: Dict,
        workflow: List,
        wait_result=False,
        wait_timeout = CLIENT_WAIT_TIMEOUT_SEC,
        wait_polling = CLIENT_WAIT_POLLING_PERIOD_SEC,
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
            if self.expire_delay:
                keydb_expiremember(self._redis, "task_data", task.id, delay=self.expire_delay)
            
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
