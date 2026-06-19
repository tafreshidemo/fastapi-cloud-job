# Design

## Architecture

The system uses FastAPI for HTTP, PostgreSQL as the source of truth, SQLAlchemy inside repositories, a mandatory async Unit of Work for transactions, Redis for best-effort cache/rate-limit/SSE support, and RabbitMQ for durable execution dispatch through the transactional outbox.

## Scaling by 100x

Scale horizontally by running more API replicas for request throughput, more worker replicas for execution throughput, and more outbox-publisher replicas for dispatch throughput. Outbox claims use `FOR UPDATE SKIP LOCKED`, so publisher replicas can scale without duplicating the same row.

The main bottlenecks are PostgreSQL connection pools, transaction latency, and lock contention on hot rows. RabbitMQ queue throughput becomes relevant once the outbox publisher is saturated, and Redis hot keys matter primarily for rate limiting and cache version counters. SSE connections scale with concurrent clients and should be treated as a separate resource budget from request throughput.

At 100x growth, the next step is better database and queue sizing, not a microservice split. Add read replicas only where the workload has a clear read-heavy split and keep the current monolith until there is a real boundary pressure, because premature service decomposition would add failure modes without removing the PostgreSQL source of truth.

## RabbitMQ Failure

If RabbitMQ is unavailable, the API still commits the Job and the OutboxEvent in PostgreSQL. The Job remains `pending`, and the outbox publisher retries the publish later with backoff. Publisher confirms matter because they narrow the uncertainty window between broker acceptance and database commit, but there is still a small duplicate-publish window if the commit fails after the confirm. That is acceptable because the worker path is idempotent and external side effects are expected to tolerate at-least-once delivery.

## Worker Failure

If a worker dies after claiming a job, manual ACK keeps the message unacked until the relevant DB state is committed. The broker can redeliver the message, but the execution lease prevents a stale worker from finishing after the job has been reclaimed. Stale-running recovery re-locks the owner row and then the job row, applies the recovery transition, and fences the old execution token.

Production long-running handlers should heartbeat or otherwise update lease state before the lease expires. Without that, a live worker can be considered stale and the job may be recovered by another worker.

## Duplicate Processing

Duplicate RabbitMQ deliveries are expected because the system is at-least-once, not exactly-once. There is no exactly-once claim path. Instead, the worker uses row locks, conditional state transitions, and execution-token fencing so only one consumer can make forward progress on a given job attempt.

Terminal, pending-phantom, and already-running duplicate messages are ACKed without re-execution. Any external side-effect handler must still be idempotent because duplicate publish or duplicate delivery can happen around crash and commit-failure boundaries.

## Redis Failure

Redis is best-effort only. If Redis is unavailable, cache-aside falls back to PostgreSQL, rate limiting fails open, and SSE can degrade by closing cleanly or emitting a reconnect hint. Redis-backed data is TTL-limited, so staleness is bounded even when Redis returns outdated list-cache entries. PostgreSQL remains the source of truth in all cases.

## More Than 100 Million Jobs

At this scale, the current model still works functionally, but operational constraints dominate. The main requirements become:

- keyset pagination only, never offset pagination;
- composite indexes that match the `created_at DESC, id DESC` access pattern;
- partial indexes for state-specific query paths;
- range or monthly partitioning by `created_at` for history tables;
- retention or archive jobs for job logs and old outbox rows;
- BRIN indexes for append-heavy time ranges where appropriate;
- close vacuum/analyze discipline and awareness of index write amplification.

The design does not depend on in-memory state, so the scaling work is primarily data-shape and operational, not architectural. PostgreSQL read replicas can help read-only list traffic, but they do not remove the need for good write-path tuning or lock discipline.

## Core Boundaries

- Routes stay thin.
- Application services orchestrate business behavior.
- Repositories own SQLAlchemy persistence.
- Unit of Work owns transaction boundaries.
- Workers and publishers operate through explicit application/infrastructure adapters.
