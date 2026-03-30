# taskiq-sqlalchemy

A database-agnostic [TaskIQ](https://taskiq-python.github.io/) broker and result backend
built on top of **SQLAlchemy async engines**.

Works with any database that has an async SQLAlchemy driver.
First-class support for PostgreSQL, Oracle and MSSQL; everything else falls back to a
polling-based broker automatically.

---

## Features

| | |
|---|---|
| **DB-agnostic result backend** | Stores task results in any SQL database — SQLite, PostgreSQL, Oracle, and more |
| **Push-based broker for PostgreSQL** | via `LISTEN / NOTIFY` (asyncpg or psycopg) |
| **Push-based broker for Oracle** | via Oracle Advanced Queuing (AQ) |
| **Push-based broker for MSSQL** |via SQL Server Service Broker |
|  **Polling fallback broker** | Works on any async SQLAlchemy engine — SQLite for local dev, MySQL and others |

---

## Supported Databases

### Broker

| Database | Transport | Driver extras | Status |
|---|---|---|---|
| **PostgreSQL** | `LISTEN / NOTIFY` (push) | `postgresql-asyncpg`, `postgresql-psycopg` | ✅ Supported |
| **Oracle** | Advanced Queuing — AQ (push) | `oracle-oracledb` | ✅ Supported |
| **MSSQL** | Service Broker (push) | `mssql-aioodbc` | ✅ Supported |
| **SQLite** | Polling | `sqlite-aiosqlite` | ✅ Supported (dev/test) |
| Any other async dialect | Polling | _(driver of your choice)_ | ✅ Supported via fallback |

### Result Backend

The result backend stores serialised `TaskiqResult` objects in a plain SQL table. 
It is **fully database-agnostic** - any dialect that works with SQLAlchemy async will work unchanged.

| Database | Status |
|---|---|
| SQLite | ✅ |
| PostgreSQL | ✅ |
| Oracle | ✅ |
| Any async SQLAlchemy dialect | ✅ |

### Scheduler Source

> **Roadmap** — A `SchedulerSource` implementation backed by a SQL table
> (cron expressions, one-off schedules) is planned.

---

## Installation

```bash
# SQLite only (zero external services — great for dev/testing)
pip install taskiq-sqlalchemy[sqlite-aiosqlite]

# PostgreSQL with asyncpg
pip install taskiq-sqlalchemy[postgresql-asyncpg]

# PostgreSQL with psycopg3
pip install taskiq-sqlalchemy[postgresql-psycopg]

# Oracle
pip install taskiq-sqlalchemy[oracle-oracledb]

# SQL Server (Service Broker)
pip install taskiq-sqlalchemy[mssql-aioodbc]

# Everything
pip install taskiq-sqlalchemy[all]
```

With **uv**:

```bash
uv add taskiq-sqlalchemy[postgresql-asyncpg]
```

---

## Quick Start

### 1. Configure the Manager

`SQLAlchemyManager` owns the ORM table classes and the engine.
Pass your `DeclarativeBase` subclass so the tables are registered
in your metadata and can be created/migrated alongside your own models.

```python
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from taskiq_sqlalchemy.manager import SQLAlchemyManager

class Base(DeclarativeBase):
    pass

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")

manager = SQLAlchemyManager(base_classes=(Base,))
manager.configure(engine=engine)

# Create tables (or use Alembic)
async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)
```

### 2. Set up the Broker

```python
from taskiq_sqlalchemy.broker import SQLAlchemyBroker

broker = SQLAlchemyBroker(manager, channel_name="default")
```

`SQLAlchemyBroker` automatically selects the right transport adapter for the
connected dialect (see [Broker Internals](#broker-internals) below).

### 3. Set up the Result Backend

```python
from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend

result_backend = SQLAlchemyResultBackend(manager)
broker = broker.with_result_backend(result_backend)
```

### 4. Define and run tasks

```python
import asyncio

@broker.task
async def add(a: int, b: int) -> int:
    return a + b

async def main() -> None:
    await broker.startup()

    task = await add.kiq(1, 2)
    result = await task.wait_result(timeout=10)
    print(result.return_value)   # 3

    await broker.shutdown()

asyncio.run(main())
```

Run a worker in a separate process:

```bash
taskiq worker myapp:broker
```

---

## Broker Internals

`SQLAlchemyBroker` separates two concerns:

| Concern | Component |
|---|---|
| **Persistence** | `kick()` inserts a row into the queue table, then commits |
| **Wakeup signal** | `DialectAdapter.notify()` tells workers a new row is waiting |

The commit happens *before* the notify, so a worker that wakes up immediately
will always find the row in the database.

### DialectAdapter

```
DialectAdapter (ABC)
 ├── PostgresDialectAdapter   — LISTEN/NOTIFY via asyncpg
 ├── OracleDialectAdapter     — Advanced Queuing (AQ) via oracledb Thin
 ├── MSSQLDialectAdapter      — Service Broker WAITFOR RECEIVE via aioodbc
 └── PollingAdapter           — periodic SELECT … FOR UPDATE SKIP LOCKED
```

The correct adapter is chosen automatically by `resolve_adapter()` based on
`engine.dialect.name`. You can also pass a custom adapter directly:

```python
from taskiq_sqlalchemy.adapters.polling import PollingAdapter

adapter = PollingAdapter(engine, queue_cls=manager.queue_cls, poll_interval=1.0)
broker = SQLAlchemyBroker(manager, adapter=adapter)
```

### PostgreSQL — LISTEN / NOTIFY

PostgreSQL's `LISTEN / NOTIFY` is a native, push-based pub/sub mechanism.
`PostgresDialectAdapter` holds a **dedicated raw asyncpg connection** separate
from the SQLAlchemy pool (the pool connection would be returned after each
statement, breaking the LISTEN subscription).

Notifications are bridged into the async generator via an
**anyio `MemoryObjectStream`** pair:

```
asyncpg callback (sync)  →  send_stream.send_nowait()  →  recv_stream  →  broker.listen()
```

This is fully backend-agnostic (asyncio and trio) and provides structured
lifecycle: closing `send_stream` on shutdown propagates `EndOfStream` to the
consumer cleanly.

Workers call `worker_startup()` to acquire the dedicated listener connection
and `worker_shutdown()` to release it.

### Oracle — Advanced Queuing (AQ)

Oracle AQ is Oracle's native message-queuing subsystem.
`OracleDialectAdapter` uses **oracledb Thin mode** — no Oracle Instant Client
or native libraries are needed on the worker host.

Each `channel` maps to an AQ queue created via `DBMS_AQADM`. The adapter:

1. Creates the queue table + queue on first use (`ensure_queue()`), idempotently.
2. `notify()` enqueues a RAW message with `ENQ_IMMEDIATE` (committed before the
   outer transaction returns, so workers see it right away).
3. `listen()` runs a `deqone()` loop with `DEQ_NO_WAIT` and sleeps 0.5 s between
   empty polls. It exits cleanly when `_stop_event` is set.

> **Thin mode requirement** — `broker_startup()` raises `RuntimeError` if
> `oracledb.is_thin_mode()` returns `False`. Do not call
> `oracledb.init_oracle_client()` in your application when using this adapter.

### MSSQL — Service Broker

SQL Server's Service Broker is a built-in, durable, transactional messaging
subsystem.  `MSSQLDialectAdapter` uses `aioodbc` (async ODBC) and requires no
external broker process.

Each `channel` maps to a pair of Service Broker objects created via
`ensure_queue()` (idempotent):

- **Queue** — `taskiq_sb_<channel>`
- **Service** — `taskiq_svc_<channel>`

One long-lived **dialog conversation** is opened per channel in
`worker_startup()` and reused for all subsequent `notify()` calls, avoiding
`BEGIN DIALOG` overhead on every `kick()`.

`listen()` runs a `WAITFOR (RECEIVE TOP(1) …), TIMEOUT 500` loop.  A 500 ms
timeout means the worst-case shutdown delay is 500 ms; when `_stop_event` is
set the loop exits immediately on the next timeout.

```
mssql+aioodbc://sa:Password1@localhost:1433/taskiq?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

> **Service Broker must be enabled** on the target database:
> ```sql
> ALTER DATABASE taskiq SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
> ```

### Polling Fallback

`PollingAdapter` is used for any dialect that has no native pub/sub (SQLite, etc.).

```
while not stop:
    SELECT task_id FROM queue
    WHERE channel = :channel
    ORDER BY id
    LIMIT 25
    FOR UPDATE SKIP LOCKED
    → yield each task_id
    sleep(poll_interval)   # default: 1 s
```

`FOR UPDATE SKIP LOCKED` ensures that multiple workers never claim the same row,
even under concurrent load. On SQLite (which lacks `SKIP LOCKED`) the adapter
falls back to a plain `DELETE … RETURNING` which is atomic at the SQLite WAL level.

`notify()` is a no-op for this adapter — `kick()` still inserts the row; workers
simply discover it on the next poll cycle.

---

## Configuration Reference

### `SQLAlchemyManager`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `base_classes` | `Sequence[type]` | `()` | `DeclarativeBase` subclass(es) for table registration |
| `queue_cls` | `type[TaskiqQueueMixin]` | auto | Bring your own queue ORM class |
| `result_cls` | `type[TaskiqResultMixin]` | auto | Bring your own result ORM class |
| `schedule_cls` | `type[TaskiqScheduleMixin]` | auto | Bring your own schedule ORM class |

### `SQLAlchemyBroker`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `manager` | `SQLAlchemyManager` | required | Bound manager |
| `channel_name` | `str` | `"taskiq"` | Logical queue channel name |
| `adapter` | `DialectAdapter \| None` | auto | Override the transport adapter |

### `SQLAlchemyResultBackend`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `manager` | `SQLAlchemyManager` | required | Bound manager |
| `keep_results` | `bool` | `True` | If `False`, the result row is deleted after the first `get_result()` call |
| `serializer` | `TaskiqSerializer` | `PickleSerializer()` | Swap in any `TaskiqSerializer` implementation |

---

## Running Tests

```bash
# Generic tests only (SQLite, no external services)
uv run pytest tests/ -m "not postgresql and not oracle"

# PostgreSQL tests (requires a running Postgres)
docker compose -f docker/docker-compose.yml up -d postgres
uv run pytest tests/ -m "postgresql"

# Oracle tests (requires a running Oracle XE)
docker compose -f docker/docker-compose.yml up -d oracle
uv run pytest tests/ -m "oracle"

# Everything at once
uv run pytest tests/
```

---

## Requirements

- Python 3.9+
- TaskIQ 0.11.7+
- SQLAlchemy 2.0+
- anyio 4+

---

## License

MIT — see [LICENSE](LICENSE).
