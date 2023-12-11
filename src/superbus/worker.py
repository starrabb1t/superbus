from .updater import *

WORKER_POLLING_PERIOD_SEC = 0.1

class Worker:
    def __init__(
        self,
        redis_host,
        redis_port=DEFAULT_REDIS_PORT,
        redis_password = None,
        polling_period = WORKER_POLLING_PERIOD_SEC
    ):
        
        if redis_password:
            self._redis = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        else:
            self._redis = redis.Redis(host=redis_host, port=redis_port)

        self.polling_period = polling_period
        self.updater = StatusUpdater(self._redis)

        logging.info("worker ready!")

    def run(self, operators: dict):

        for op_name in operators:
            self._redis.sadd("REGISTERED_OPERATORS", op_name)

        task = None

        while True:

            try:
                time.sleep(self.polling_period)  # slightly reduce cpu usage while wait

                for op_name in operators:

                    if self._redis.llen(op_name) != 0:

                        task_id = self._redis.rpop(op_name)
                        logger.info(f"received task '{task_id}'")
                        
                        task = self.updater.get_task_by_id(task_id)
                        task_data = self.updater.get_task_data_by_id(task_id)

                        task.stage = op_name
                        self.updater.set_in_progress(task)

                        operator_func = operators[op_name]
                        result_data = operator_func(task_data)
                        result_data_json = json.dumps(result_data)
                        
                        self._redis.hset("task_data", task.id, result_data_json)
                        keydb_expiremember(self._redis, "task_data", task.id)

                        if task.workflow.index(op_name) == len(task.workflow) - 1:
                            self.updater.set_success(task)
                            logger.info(f"task succeeded '{task.id}'")

                            task_dict = task.dict()
                            task_dict["data"] = result_data

                            if task.webhook:
                                self.updater.send_webhook_post(task_dict, task.webhook)

                        else:
                            next_op_name = task.workflow[
                                task.workflow.index(op_name) + 1
                            ]
                            task.stage = next_op_name
                            self.updater.set_created(task)

                            self._redis.lpush(next_op_name, task.id)

                            logger.info(
                                f"stage completed '{task.id}', pushed to '{next_op_name}'"
                            )

            except Exception as e:

                try:
                    self.updater.set_error(str(e), task)
                    logger.error(
                        f"task '{task.id} failed'. Traceback: {traceback.format_exc()}"
                    )
                except:
                    logger.error(
                        f"incorrect task. Check if task is not None. Traceback: {traceback.format_exc()}"
                    )

                try:
                    if task.webhook:
                        self.updater.send_webhook_post(task, task.webhook)
                except:
                    logger.error(f"post to webhook failed! Traceback: {traceback.format_exc()}")
