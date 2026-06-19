# Cloud Job API

Asynchronous job execution API built with FastAPI, PostgreSQL, Redis, RabbitMQ, and a transactional outbox.

## Project Overview

The API accepts authenticated job submissions, persists them in PostgreSQL, publishes dispatch work through RabbitMQ via the outbox, and exposes job status, logs, cancellation, listing, and SSE updates. PostgreSQL is the source of truth. Redis is best-effort for rate limiting, cache invalidation, and status fanout.

The transactional outbox is a deliberate trade-off: job creation commits first, dispatch happens later, and RabbitMQ confirmation is separated from the HTTP request. That gives durable acceptance of work without making the API depend on broker availability. The cost is at-least-once delivery and possible republish after crash or commit-failure boundaries, so handlers must be idempotent.

## Architecture Summary

```text
Client -> FastAPI routes -> application services -> repositories -> PostgreSQL
                                      |                    |
                                      |                    +--> transactional outbox
                                      |                                   |
                                      +--> Redis (rate limit/cache/SSE)   +--> RabbitMQ -> worker
```

## Technology Stack

- Python 3.12
- FastAPI
- SQLAlchemy 2.x
- Alembic
- PostgreSQL
- Redis 7.4
- RabbitMQ 4.1
- `pwdlib[argon2]`

## Prerequisites

- Docker and Docker Compose
- Python 3.12 if you want to run local tooling outside containers
- A shell that can run the commands below

## Environment Setup

Copy `.env.example` to `.env` and adjust secrets or local ports if needed. The application expects `DATABASE_URL`, `REDIS_URL`, `RABBITMQ_URL`, and `JWT_SECRET`.

Runtime services use `cloud_job`. Tests use `cloud_job_test` through `TEST_DATABASE_URL`.

## Docker Compose Startup

```bash
docker compose up --build
```

This starts:

- `api`
- `worker`
- `outbox-publisher`
- `migrate`
- `postgres`
- `redis`
- `rabbitmq`

## Migration Commands

Run migrations through the compose service:

```bash
docker compose run --rm migrate
```

Or inside the running API container:

```bash
docker compose exec api alembic upgrade head
```

## Admin Creation

Create or update the initial admin user with the CLI:

```bash
docker compose exec api python -m app.cli.create_admin --email admin@example.com --password 'change-me'
```

## Test, Lint, and Type-Check

```bash
ruff check .
ruff format --check .
mypy app
pytest -q
docker compose exec api pytest -q
docker compose run --rm test
```

For safe Docker execution, the runtime API, worker, and outbox publisher continue to use `cloud_job`, while the test harness uses `cloud_job_test` and never resets the runtime database.

## API Examples

### Register

```bash
curl -s -X POST http://localhost:8000/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"user@example.com","password":"Secret123!"}'
```

### Login

```bash
curl -s -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"user@example.com","password":"Secret123!"}'
```

### Create Job

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H 'Authorization: Bearer <token>' \
  -H 'Idempotency-Key: job-123' \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"duration_seconds":5}}'
```

### List Jobs

```bash
curl -s 'http://localhost:8000/jobs?limit=20&cursor=<cursor>' \
  -H 'Authorization: Bearer <token>'
```

### Cancel Job

```bash
curl -s -X POST http://localhost:8000/jobs/<job_id>/cancel \
  -H 'Authorization: Bearer <token>'
```

### Get Job

```bash
curl -s http://localhost:8000/jobs/<job_id> \
  -H 'Authorization: Bearer <token>'
```

### Logs

```bash
curl -s http://localhost:8000/jobs/<job_id>/logs \
  -H 'Authorization: Bearer <token>'
```

### SSE

```bash
curl -N http://localhost:8000/jobs/<job_id>/events \
  -H 'Authorization: Bearer <token>'
```

## RabbitMQ Management Access

RabbitMQ Management UI is available at `http://localhost:15672`.

- Username: `guest`
- Password: `guest`

## Job Types and Payloads

- `sleep`: `{ "payload": { "duration_seconds": 1-30 } }`
- `success`: `{ "payload": {} }`
- `failure`: `{ "payload": { "message": "optional string up to 500 chars" } }`

No arbitrary command execution is allowed. Handlers are restricted to the approved job types above.

Job creation remains `pending` until RabbitMQ-confirmed dispatch moves it forward. That means the user can see a durable job record before execution begins.

## State Definitions

- `pending`
- `queued`
- `running`
- `completed`
- `failed`
- `cancelled`

## Retry Semantics

Jobs get one initial execution plus up to three retries. Retry delays are 5, 15, and 30 seconds. After the fourth failed attempt, the job becomes `failed`.

Execution is at-least-once, not exactly-once. External systems touched by job handlers must tolerate duplicate delivery and duplicate publish boundaries.

## Acceptance Coverage

The repository includes explicit tests for:

- 5 concurrent requests sharing one idempotency key create exactly one job.
- Four concurrent worker claims for one user yield at most three running jobs.
- Duplicate RabbitMQ messages do not execute the same active or terminal job twice.
- Commit failure before ACK causes safe redelivery.
- Publisher success plus DB commit failure may republish but cannot duplicate execution.
- Worker crash plus lease expiry recovers a job.
- Stale worker completion is ignored by execution-token fencing.
- Two outbox publishers do not publish the same claimed row concurrently.
- Redis outage does not prevent list fallback or job creation.
- Cache invalidation occurs for worker-originated status changes.

## Known Limitations

- Execution is at-least-once, so external handlers must be idempotent.
- Redis fanout and cache invalidation are best-effort.
- SSE is observation only; PostgreSQL remains authoritative.
- RabbitMQ delivery and worker processing are durable, but message replay is still possible after crashes or commit failures.
