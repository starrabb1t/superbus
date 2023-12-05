import logging
import json
from pydantic import BaseModel
from typing import Dict, List, Optional

REDIS_KEY_EXP_TIME_SEC = 6 * 3600
DEFAULT_REDIS_PORT = 6379
TIMESTAMP_FORMAT_STR = "%Y-%m-%dT%H:%M:%S.%f"

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
    timestamp: Optional[str]    # latest timestamp
    status: Optional[str]       # current status
    stage: Optional[str]        # current stage (name of operator)
    error: Optional[str]        # error message (empty if success)
    webhook: Optional[str]      # webhook url to send POST with result

def keydb_expiremember(keydb_instance, key, subkey, delay=REDIS_KEY_EXP_TIME_SEC, unit='s'):
    """
    Set timeout on a subkey. This feature only available on KeyDB
    https://docs.keydb.dev/docs/commands/#expiremember
    :param key: key added by `SADD key [subkeys]`
    :param subkey: subkey on the set
    :param delay: timeout
    :param unit: `s` or `ms`
    :return: 0 if the timeout was set, otherwise 0
    """
    args = [key, subkey, delay]
    if unit is not None and unit not in ['s', 'ms']:
        raise ValueError("`unit` must be s or ms")

    if unit:
        args.append(unit)

    return keydb_instance.execute_command('EXPIREMEMBER', *args)