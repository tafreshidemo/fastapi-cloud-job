from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime

import pytest
from aio_pika import DeliveryMode, ExchangeType, Message
from aio_pika.exceptions import QueueEmpty

from app.infrastructure.rabbitmq.connection import connect_robust_with_retry
from app.infrastructure.rabbitmq.topology import (
    JOBS_EXECUTE_ROUTING_KEY,
    RabbitMQDispatchPublisher,
    RabbitMQTopology,
)
from app.models.outbox_event import OutboxEvent
from app.schemas.outbox import DispatchOutboxMessage
from tests.support import get_test_rabbitmq_runtime_url

TEST_RABBITMQ_URL = get_test_rabbitmq_runtime_url()


async def get_dead_letter_with_retry(dead_queue: object, *, attempts: int = 20) -> object:
    queue = dead_queue
    for attempt in range(attempts):
        try:
            return await queue.get(timeout=1, fail=True)
        except QueueEmpty:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.25)


async def get_queue_message_with_retry(queue: object, *, attempts: int = 20) -> object:
    for attempt in range(attempts):
        try:
            return await queue.get(timeout=1, fail=True)
        except QueueEmpty:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.25)


@pytest.mark.asyncio
async def test_rabbitmq_topology_and_publisher_confirms_on_live_broker() -> None:
    connection = await connect_robust_with_retry(TEST_RABBITMQ_URL)
    try:
        channel = await connection.channel(publisher_confirms=True)
        topology = RabbitMQTopology()
        await topology.declare(channel)

        test_exchange_name = f"test.jobs.direct.{uuid.uuid4()}"
        test_queue_name = f"test.jobs.execute.{uuid.uuid4()}"
        test_dead_queue_name = f"test.jobs.dead.{uuid.uuid4()}"
        test_dlx_name = f"test.jobs.dlx.{uuid.uuid4()}"
        exchange = await channel.declare_exchange(
            test_exchange_name,
            ExchangeType.DIRECT,
            durable=False,
        )
        dead_letter_exchange = await channel.declare_exchange(
            test_dlx_name,
            ExchangeType.DIRECT,
            durable=False,
        )
        execute_queue = await channel.declare_queue(
            test_queue_name,
            durable=False,
            arguments={
                "x-dead-letter-exchange": test_dlx_name,
                "x-dead-letter-routing-key": test_dead_queue_name,
            },
        )
        dead_queue = await channel.declare_queue(test_dead_queue_name, durable=False)
        await execute_queue.bind(exchange, routing_key=JOBS_EXECUTE_ROUTING_KEY)
        await dead_queue.bind(dead_letter_exchange, routing_key=test_dead_queue_name)
        await execute_queue.purge()
        await dead_queue.purge()

        created_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
        job_id = uuid.uuid4()
        outbox_event = OutboxEvent(
            id=uuid.uuid4(),
            aggregate_id=job_id,
            event_type="job.dispatch",
            payload={},
            available_at=created_at,
            publish_attempts=0,
        )
        message = DispatchOutboxMessage(
            event_id=outbox_event.id,
            job_id=job_id,
            kind="dispatch",
            created_at=created_at,
        )

        publisher = RabbitMQDispatchPublisher(exchange)
        await publisher.publish_dispatch(outbox_event=outbox_event, message=message)

        delivery = await get_queue_message_with_retry(execute_queue)
        assert delivery.message_id == str(outbox_event.id)
        assert delivery.correlation_id == str(job_id)
        assert delivery.content_type == "application/json"
        assert delivery.type == "dispatch"
        assert delivery.headers["kind"] == "dispatch"
        assert delivery.headers["event_type"] == "job.dispatch"
        assert json.loads(delivery.body.decode("utf-8")) == message.model_dump(mode="json")

        await delivery.reject(requeue=False)

        dead_letter = await get_dead_letter_with_retry(dead_queue)
        assert dead_letter.message_id == str(outbox_event.id)
        assert dead_letter.correlation_id == str(job_id)
        assert json.loads(dead_letter.body.decode("utf-8")) == message.model_dump(mode="json")
        await dead_letter.ack()
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_malformed_message_is_routed_to_dead_letter_queue_on_live_broker() -> None:
    connection = await connect_robust_with_retry(TEST_RABBITMQ_URL)
    try:
        channel = await connection.channel(publisher_confirms=True)
        topology = RabbitMQTopology()
        await topology.declare(channel)

        test_exchange_name = f"test.jobs.direct.{uuid.uuid4()}"
        test_queue_name = f"test.jobs.execute.{uuid.uuid4()}"
        test_dead_queue_name = f"test.jobs.dead.{uuid.uuid4()}"
        test_dlx_name = f"test.jobs.dlx.{uuid.uuid4()}"
        exchange = await channel.declare_exchange(
            test_exchange_name,
            ExchangeType.DIRECT,
            durable=False,
        )
        dead_letter_exchange = await channel.declare_exchange(
            test_dlx_name,
            ExchangeType.DIRECT,
            durable=False,
        )
        execute_queue = await channel.declare_queue(
            test_queue_name,
            durable=False,
            arguments={
                "x-dead-letter-exchange": test_dlx_name,
                "x-dead-letter-routing-key": test_dead_queue_name,
            },
        )
        dead_queue = await channel.declare_queue(test_dead_queue_name, durable=False)
        await execute_queue.bind(exchange, routing_key=JOBS_EXECUTE_ROUTING_KEY)
        await dead_queue.bind(dead_letter_exchange, routing_key=test_dead_queue_name)
        await execute_queue.purge()
        await dead_queue.purge()

        malformed_message_id = str(uuid.uuid4())
        malformed_job_id = str(uuid.uuid4())
        await exchange.publish(
            Message(
                body=b'{"kind":"dispatch"}',
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=malformed_message_id,
                correlation_id=malformed_job_id,
                type="dispatch",
                headers={"kind": "dispatch", "event_type": "job.dispatch"},
            ),
            routing_key=JOBS_EXECUTE_ROUTING_KEY,
        )

        delivery = await get_queue_message_with_retry(execute_queue)
        await delivery.reject(requeue=False)

        dead_letter = await get_dead_letter_with_retry(dead_queue)
        assert dead_letter.message_id == malformed_message_id
        assert dead_letter.correlation_id == malformed_job_id
        assert json.loads(dead_letter.body.decode("utf-8")) == {"kind": "dispatch"}
        await dead_letter.ack()
    finally:
        await connection.close()
