# Superbus 🚏  
**Redis-based bus for microservice communication handling long-running tasks**

---

## 📦 Installation

To install via **pip**:
```bash
pip install superbus
```

To install dependencies and use the package with **Poetry**:
```bash
poetry install
```

To build the package:
```bash
poetry build
```

---

## 🚀 Getting Started

1. **Run Redis** (locally or in Docker):
   ```bash
   docker run --name superbus-redis -p 6379:6379 -d redis:7
   ```

2. **Start a worker:**
   ```bash
   python examples/dummy/worker.py
   ```

3. **Start a client and create a task:**
   ```bash
   python examples/dummy/client.py
   ```

4. **Clean up completed and orphaned tasks (in a Python console):**
   ```python
   from superbus.client import Client
   c = Client(redis_host="localhost")
   c.clearCompleted()
   ```

For more advanced usage examples, see the [`examples`](./examples) directory.

---

## 🧠 Concept
**Superbus** is a lightweight library (bus) designed for asynchronous communication between microservices using Redis.  
It enables you to:
- distribute tasks between workers,
- track task execution state,
- manage data processing pipelines,
- use webhooks for result notifications.

---

## 🧩 Core Classes

### `Client`
Used by client microservices to enqueue tasks.  
**Methods:**
- `pushTask(task_data, workflow, wait_result=False, webhook=None)` — creates and enqueues a task.
- `getTask(task_id)` — retrieves task state.
- `getQueue(workflow=None)` — returns queue lengths for workflow operators.
- `listTasks()` — returns a list of all active task IDs.
- `clearCompleted()` — removes completed and orphaned tasks from Redis.

---

### `Worker`
Runs on operators (pipeline workers).  
**Methods:**
- `run(operators: dict)` — registers operator functions and continuously processes tasks from Redis.

**Example:**
```python
from superbus.worker import Worker

def add(data):
    data["x"] += 1
    return data

worker = Worker(redis_host="localhost")
worker.run({"adder": add})
```

---

### `StatusUpdater`
Service component for managing task metadata.  
- supports statuses: `CREATED`, `IN PROGRESS`, `SUCCESS`, `ERROR`, `TIMEOUT`  
- updates and serializes tasks in Redis

---

### `TaskModel`
Pydantic model representing a task:
```python
class TaskModel(BaseModel):
    id: str
    workflow: List[str]
    timestamp: Optional[str]
    status: Optional[str]
    stage: Optional[str]
    error: Optional[str]
    webhook: Optional[str]
```

---

## 🧰 Utilities
- `keydb_expiremember` — safely works with both KeyDB and Redis.  
  - In **KeyDB**, uses `EXPIREMEMBER`.  
  - In **Redis**, TTL for subkeys is ignored (skipped).
- `get_logger()` — structured JSON logger.

---

## 🧪 Example end-to-end execution
```python
from superbus.client import Client

c = Client(redis_host="localhost")
task = c.pushTask({"text": "Hi"}, ["adder"], wait_result=True)
print(task)
```

---

## 📜 License
MIT © [starrabb1t](https://github.com/starrabb1t)
