import logging
import json
from pydantic import BaseModel
from typing import Dict, List, Optional
import signal
import traceback

CLIENT_WAIT_TIMEOUT_SEC = 300
CLIENT_WAIT_POLLING_PERIOD_SEC = 1
DEFAULT_WEBHOOK_TIMEOUT_SEC = 5
WORKER_POLLING_PERIOD_SEC = 0.1
REDIS_KEY_EXP_TIME_SEC = 3600
DEFAULT_REDIS_PORT = 6379
TIMESTAMP_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%f"
REDIS_CONNECTION_TIMEOUT_SEC = 10
REDIS_HEALTH_CHECK_INTERVAL_SEC = 30

def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format=json.dumps(
            {
                "time": "%(asctime)s",
                "level": "%(levelname)s",
                "func": "%(funcName)s",
                "message": "%(message)s",
            }
        ),
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    class MessageFilter(logging.Filter):
        def filter(self, record):
            record.msg = json.dumps(record.msg)[1:-1]
            return record

    logger = logging.getLogger()
    logger.addFilter(MessageFilter())

    return logger


logger = get_logger()

class TaskModel(BaseModel):
    id: str                     # task identificator
    workflow: List              # graph of operators
    timestamp: Optional[str]=None    # latest timestamp
    status: Optional[str]=None       # current status
    stage: Optional[str]=None        # current stage (name of operator)
    error: Optional[str]=None        # error message (empty if success)
    webhook: Optional[str]=None      # webhook url to send POST with result

def keydb_expiremember(redis_instance, key, subkey, delay=REDIS_KEY_EXP_TIME_SEC, unit='s'):
    """
    Safe expiration for both KeyDB and Redis.

    - On KeyDB: uses EXPIREMEMBER
    - On Redis: fallback to normal EXPIRE (sets timeout for whole key)
    """
    try:
        # Проверяем тип движка, если ранее определён флаг
        is_keydb = getattr(redis_instance, "is_keydb", None)
        if is_keydb is None:
            try:
                info = redis_instance.info("server")
                is_keydb = "keydb_version" in info
                redis_instance.is_keydb = is_keydb
            except Exception:
                is_keydb = False

        if is_keydb:
            args = [key, subkey, delay]
            if unit is not None and unit not in ["s", "ms"]:
                raise ValueError("`unit` must be 's' or 'ms'")
            if unit:
                args.append(unit)
            return redis_instance.execute_command("EXPIREMEMBER", *args)
        else:
            # fallback — ничего не делаем на Redis
            logger.debug("Redis detected: skipping EXPIREMEMBER fallback (no subkey TTL support)")
            return None

    except Exception as e:
        logger.warning(f"keydb_expiremember fallback triggered: {e}")
        try:
            return redis_instance.expire(key, delay)
        except Exception as e2:
            logger.error(f"expire() also failed: {e2}")
            return None

class Terminator:

    def __init__(self):
        self.terminate = False
        signal.signal(signal.SIGINT, self.terminate_gracefully)
        signal.signal(signal.SIGTERM, self.terminate_gracefully)

    def terminate_gracefully(self, signum, frame):
        self.terminate = True
        logger.warning(f"got signal 0x0{signum}!")
