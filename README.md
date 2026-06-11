# FastAPI RabbitMQ all

A comprehensive FastAPI application demonstrating multiple RabbitMQ messaging patterns with SQLAlchemy (SQLite), task tracking, retry logic, and Dead Letter Queue (DLQ).



<p align="center">
  <img src="README-images/db.png" alt="Step1">
</p>


## Features

- **4 RabbitMQ Patterns**
  - **Basic Queue**: Simple point-to-point message delivery
  - **Work Queues (Competing Consumers)**: Distribute tasks among multiple workers
  - **Pub/Sub (Fanout Exchange)**: Broadcast messages to all subscribers
  - **Routing (Topic Exchange)**: Route messages based on routing keys

- **Task Management API** (CRUD)
  - Create tasks with title, description, and pattern selection
  - List tasks with pagination and status filtering
  - Get task details and real-time status
  - Update and delete tasks

- **Production Features**
  - Message persistence and durability
  - Manual acknowledgment (auto-ack disabled)
  - Retry logic with exponential backoff (up to 3 retries)
  - Dead Letter Queue (DLQ) for failed messages
  - Task status tracking (pending → processing → completed/failed)
  - Prefetch count control for fair dispatching

## Requirements

- **Python** 3.11+
- **Docker** (for RabbitMQ)
- **uv** (Python package manager) or pip

## Quick Start

### 1. Start RabbitMQ

```bash
docker run -d --name mi-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

RabbitMQ Management UI: http://localhost:15672 (guest/guest)

### 2. Install Dependencies

```bash
uv sync
```

Or with pip:

```bash
pip install -e .
```

### 3. Start the API Server

```bash
uv run python main.py
```

The API will be available at http://localhost:8000
Interactive docs at http://localhost:8000/docs

### 4. Start Workers (in a separate terminal)

```bash
uv run python workers/run_workers.py
```

## API Usage

### Create a Task (PowerShell)

```powershell
# Basic pattern
$body = @{title="Process report"; description="Generate monthly report"; pattern="basic"} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8000/tasks" -Method Post -Body $body -ContentType "application/json"

# Work Queue pattern
$body = @{title="Heavy computation"; description="Complex calculation"; pattern="work_queue"} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8000/tasks" -Method Post -Body $body -ContentType "application/json"

# Fanout pattern (broadcast to all subscribers)
$body = @{title="System notification"; description="Notify all services"; pattern="fanout"} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8000/tasks" -Method Post -Body $body -ContentType "application/json"

# Routing pattern (topic exchange)
$body = @{title="High priority task"; description="Urgent task"; pattern="routing"} | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8000/tasks" -Method Post -Body $body -ContentType "application/json"
```

### List Tasks (PowerShell)

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/tasks?page=1&page_size=20"
```

### Get Task Status (PowerShell)

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/tasks/1/status"
```

## RabbitMQ Architecture

### Exchanges
| Exchange Name | Type | Purpose |
|---|---|---|
| `tasks.direct` | direct | Basic and Work Queue patterns |
| `tasks.fanout` | fanout | Publish/Subscribe broadcast |
| `tasks.topic` | topic | Routing with wildcard matching |
| `tasks.dlx` | direct | Dead Letter Exchange |

### Queues
| Queue Name | Bound To | Routing Key | Purpose |
|---|---|---|---|
| `tasks.basic` | tasks.direct | `task.basic` | Simple queue |
| `tasks.work_queue` | tasks.direct | `task.work_queue` | Competing consumers |
| `tasks.pubsub` | tasks.fanout | `""` | Fanout broadcast |
| `tasks.routing` | tasks.topic | `task.*` | Topic routing |
| `tasks.dlq` | tasks.dlx | `dlq` | Dead letter handling |

## Project Structure

```
fastapirabbitmqt/
├── main.py                     # FastAPI app with lifespan
├── config.py                   # Settings via pydantic-settings
├── database/
│   ├── __init__.py
│   ├── session.py              # SQLAlchemy engine and session
│   └── models.py               # Task model with status enum
├── schemas/
│   ├── __init__.py
│   └── task.py                 # Pydantic request/response schemas
├── rabbitmq/
│   ├── __init__.py
│   ├── connection.py           # Singleton connection manager
│   ├── exchanges.py            # Infrastructure setup (exchanges, queues)
│   ├── producer.py             # Message publisher
│   ├── consumer.py             # Base consumer with retry/DLQ logic
│   ├── dlq.py                  # Dead letter queue consumer
│   └── workers/
│       ├── __init__.py
│       ├── basic_worker.py     # Basic queue consumer
│       ├── work_queue_worker.py # Competing consumer
│       ├── pubsub_worker.py    # Fanout subscriber
│       └── routing_worker.py   # Topic routing consumer
├── services/
│   ├── __init__.py
│   └── task_service.py         # Business logic layer
├── api/
│   ├── __init__.py
│   ├── deps.py                 # FastAPI dependencies
│   └── routes/
│       ├── __init__.py
│       └── tasks.py            # REST endpoints
└── workers/
    ├── __init__.py
    └── run_workers.py          # Launcher for all consumers
```

## Configuration

Copy `.env.example` or set environment variables:


```
DATABASE_URL=sqlite:///./tasks.db
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
MAX_RETRIES=3
RETRY_DELAY_BASE=2.0
WORKER_PREFETCH_COUNT=1
```



| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./tasks.db` | Database connection |
| `RABBITMQ_HOST` | `localhost` | RabbitMQ server |
| `RABBITMQ_PORT` | `5672` | AMQP port |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASSWORD` | `guest` | RabbitMQ password |
| `MAX_RETRIES` | `3` | Max message retries before DLQ |
| `RETRY_DELAY_BASE` | `2.0` | Base delay for exponential backoff |
| `WORKER_PREFETCH_COUNT` | `1` | Prefetch count per consumer |



### 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👨‍💻 Author / Autor

**Diego Ivan Perea Montealegre**

- GitHub: [@diegoperea20](https://github.com/diegoperea20)

---

Created by [Diego Ivan Perea Montealegre](https://github.com/diegoperea20)