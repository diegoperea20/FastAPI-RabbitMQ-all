# AGENTS.md - Project Guide for AI Agents

## Project Overview

This is a **FastAPI** application that demonstrates **4 RabbitMQ messaging patterns** using **SQLAlchemy (SQLite)** for task persistence, with retry logic, Dead Letter Queue (DLQ), and task status tracking.

**Stack**: Python 3.11+, FastAPI, SQLAlchemy 2.0 (async), aio-pika (async RabbitMQ), Pydantic v2, aiosqlite

---

## Directory Structure & Responsibilities

```
fastapirabbitmqt/
├── main.py                     # FastAPI app entry point, lifespan events
├── config.py                   # Singleton Settings via pydantic-settings
├── database/
│   ├── __init__.py             # Re-exports: Base, engine, AsyncSessionLocal, get_db, init_db, close_db, Task, TaskStatus
│   ├── session.py              # Async SQLAlchemy engine, async_sessionmaker, get_db() async generator, init_db(), close_db()
│   └── models.py               # Task model (id, title, description, status, pattern, retry_count, max_retries, error_message, timestamps)
├── schemas/
│   ├── __init__.py             # Re-exports all Pydantic models
│   └── task.py                 # TaskCreate, TaskUpdate, TaskResponse, TaskListResponse, TaskStatusResponse + enums TaskPattern, TaskStatus
├── rabbitmq/
│   ├── __init__.py             # Re-exports all RabbitMQ components
│   ├── connection.py           # RabbitMQConnectionManager singleton via aio-pika pool, channel() async context manager
│   ├── exchanges.py            # Exchange/queue/binding declarations using aio-pika, setup_infrastructure() async
│   ├── producer.py             # TaskProducer: async publish_basic(), publish_work_queue(), publish_fanout(), publish_routing(), publish_by_pattern()
│   ├── consumer.py             # BaseConsumer (abstract async), CallbackConsumer - async retry logic via aio-pika
│   ├── dlq.py                  # DLQConsumer - marks tasks as FAILED in DB (async)
│   └── workers/
│       ├── basic_worker.py     # BasicWorker - async consumer, 1s simulated work via asyncio.sleep()
│       ├── work_queue_worker.py # WorkQueueWorker - competing consumers, 2s simulated work (async)
│       ├── pubsub_worker.py    # PubSubWorker - fanout subscribers, 1s simulated work (async)
│       └── routing_worker.py   # RoutingWorker - topic routing, 1.5s simulated work (async)
├── services/
│   ├── __init__.py             # Re-exports TaskService
│   └── task_service.py         # TaskService CRUD + publish to RabbitMQ
├── api/
│   ├── __init__.py             # Re-exports tasks_router
│   ├── deps.py                 # get_task_service() dependency
│   └── routes/
│       ├── __init__.py         # Re-exports tasks_router
│       └── tasks.py            # REST: POST/GET /tasks, GET/PATCH/DELETE /tasks/{id}, GET /tasks/{id}/status
└── workers/
    └── run_workers.py          # Launches 8 workers in asyncio tasks: basic, 2x work_queue, 2x pubsub, 2x routing, dlq
```

---

## Code Conventions

- **Imports always absolute**, never relative (e.g., `from database import Task`, not `from ..database import Task`)
- **No __init__.py type annotations** - re-exports only
- **Pydantic v2** - uses `model_config = ConfigDict(from_attributes=True)` for ORM mode, `model_dump()` instead of `dict()`
- **SQLAlchemy 2.0 style** - `declarative_base()` replaced with `DeclarativeBase`
- **Type hints** used throughout, `Optional[X]` preferred over `X | None`
- **Logging** uses standard `logging.getLogger(__name__)` in every module
- **Exception handling**: workers catch exceptions, update task status to FAILED, then re-raise for consumer retry logic
- **RabbitMQ connection**: Singleton pattern via `RabbitMQConnectionManager`, with connection pool (aio-pika Pool) and auto-reconnect
- **Singleton pattern for producer/consumer** instances at module level (e.g., `producer = TaskProducer()`)

---

## Data Flow

```
Client POST /tasks
  → TaskService.create_task()
    → INSERT task in SQLite (status=pending)
    → TaskProducer.publish_by_pattern()
      → publish to RabbitMQ exchange based on pattern
        → Worker consumes message
          → UPDATE task status = processing
          → Simulate work (sleep)
          → UPDATE task status = completed
        → On failure:
          → Retry up to 3x with exponential backoff (2^retry seconds)
          → After 3 failures → NACK, message goes to DLQ
            → DLQConsumer marks task as FAILED in DB
```

---

## RabbitMQ Infrastructure

### Exchanges (4)
| Name | Type | Purpose |
|---|---|---|
| `tasks.direct` | direct | Routes by exact key: `task.basic`, `task.work_queue` |
| `tasks.fanout` | fanout | Broadcasts to all bound queues |
| `tasks.topic` | topic | Routes by pattern: `task.*`, `task.high.*` |
| `tasks.dlx` | direct | Dead Letter Exchange for failed messages |

### Queues (5)
| Name | DLX Configured? | Bound To | Routing Key |
|---|---|---|---|
| `tasks.basic` | Yes (→tasks.dlx) | tasks.direct | `task.basic` |
| `tasks.work_queue` | Yes (→tasks.dlx) | tasks.direct | `task.work_queue` |
| `tasks.pubsub` | Yes (→tasks.dlx) | tasks.fanout | `` |
| `tasks.routing` | Yes (→tasks.dlx) | tasks.topic | `task.*` |
| `tasks.dlq` | No | tasks.dlx | `dlq` |

When any non-DLQ queue rejects a message (NACK with requeue=false), it goes to `tasks.dlx` exchange → `tasks.dlq` queue.

### Message Format
```json
{
  "task_id": 1,
  "title": "Process report",
  "description": "Generate report",
  "pattern": "basic",
  "_retry_count": 0,       // Added by consumer on retry
  "_last_error": "..."     // Added by consumer on retry
}
```

Messages use `delivery_mode=2` (persistent), `content_type=application/json`, with `message_id` UUID.

---

## Key Files & Their Critical Functions

### `config.py:Settings`
- Uses `pydantic-settings` with `@lru_cache` for singleton
- Reads from `.env` file or environment variables
- **Critical defaults**: `MAX_RETRIES=3`, `RETRY_DELAY_BASE=2.0`, `WORKER_PREFETCH_COUNT=1`

### `main.py:lifespan()`
- `init_db()` - Creates SQLite tables
- `setup_infrastructure()` - Declares RabbitMQ exchanges, queues, bindings
- `connection_manager.close()` - Closes RabbitMQ on shutdown

### `rabbitmq/connection.py:RabbitMQConnection`
- Singleton via `aio_pika.pool.Pool`
- Auto-reconnect with robust connection pool
- `channel()` async context manager yields aio-pika channel from pool

### `rabbitmq/consumer.py:BaseConsumer`
- Abstract base class for all workers
- `_process_message()`: Deserializes JSON, calls `handle_message()`, ack on success
- `_handle_failure()`: If `_retry_count < max_retries`, republishes with incremented `_retry_count` and sleeps for exponential backoff; else NACK to DLQ
- **CRITICAL**: `handle_message()` is abstract - must be implemented by subclasses

### `rabbitmq/exchanges.py:setup_infrastructure()`
- Must be called before any publish/consume to ensure topology exists
- All non-DLQ queues have `x-dead-letter-exchange: tasks.dlx` and `x-dead-letter-routing-key: dlq`

### `workers/run_workers.py`
- Launches 8 workers in separate asyncio tasks
- Handles SIGINT/SIGTERM for graceful shutdown
- Workers list: `[basic_worker, work_queue_worker_1, work_queue_worker_2, pubsub_subscriber_1, pubsub_subscriber_2, routing_worker_1, routing_worker_2, dlq_consumer]`

---

## How to Run

```bash
# Terminal 1: API server
uv run python main.py

# Terminal 2: Workers
uv run python workers/run_workers.py
```

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Root info with pattern list |
| GET | `/health` | Health check (DB + RabbitMQ) |
| POST | `/tasks` | Create task, publishes to RabbitMQ |
| GET | `/tasks` | List tasks with pagination & status filter |
| GET | `/tasks/{id}` | Get task details |
| GET | `/tasks/{id}/status` | Get task status (lightweight) |
| PATCH | `/tasks/{id}` | Update task |
| DELETE | `/tasks/{id}` | Delete task |

---

## Common Changes / Extensions

### Adding a new pattern
1. Add enum value in `schemas/task.py:TaskPattern`
2. Add publish method in `rabbitmq/producer.py:TaskProducer`
3. Update `publish_by_pattern()` switch
4. Create worker in `rabbitmq/workers/`
5. Add binding in `rabbitmq/exchanges.py:BINDINGS`
6. Register worker in `workers/run_workers.py`

### Changing retry behavior
- Update `MAX_RETRIES` and `RETRY_DELAY_BASE` in `config.py`
- Or pass different values to `BaseConsumer.__init__()`

### Adding a new field to Task model
1. Add column in `database/models.py:Task`
2. Add field in `schemas/task.py:TaskCreate` / `TaskResponse`
3. Run - `init_db()` handles table creation automatically (SQLite)

### Testing
The project uses pytest. Tests are located alongside the code. Run with:
```bash
uv run pytest
```

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `fastapi` | >=0.109.0 | Web framework |
| `uvicorn[standard]` | >=0.27.0 | ASGI server |
| `sqlalchemy` | >=2.0.0 | ORM + database |
| `aio-pika` | >=9.0.0 | Async RabbitMQ client |
| `aiosqlite` | >=0.19.0 | Async SQLite driver |
| `pydantic` | >=2.5.0 | Data validation |
| `pydantic-settings` | >=2.1.0 | Settings management |
| `python-dotenv` | >=1.0.0 | .env file loading |
