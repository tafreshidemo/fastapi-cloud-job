from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import pytest
from aio_pika import DeliveryMode, ExchangeType, Message

from app.infrastructure.rabbitmq.topology import (
    JOBS_DEAD_QUEUE_NAME,
    JOBS_DEAD_ROUTING_KEY,
    JOBS_DIRECT_EXCHANGE_NAME,
    JOBS_DLX_EXCHANGE_NAME,
    JOBS_EXECUTE_QUEUE_NAME,
    JOBS_EXECUTE_ROUTING_KEY,
    RabbitMQDispatchPublisher,
    RabbitMQTopology,
)
from app.models.outbox_event import OutboxEvent
from app.schemas.outbox import DispatchOutboxMessage


class FakeExchange:
    def __init__(self, name: str) -> None:
        self.name = name
        self.published: list[tuple[Message, str]] = []

    async def publish(self, message: Message, routing_key: str) -> None:
        self.published.append((message, routing_key))


class FakeQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.bind_calls: list[tuple[FakeExchange, str]] = []

    async def bind(self, exchange: FakeExchange, routing_key: str) -> None:
        self.bind_calls.append((exchange, routing_key))


class FakeChannel:
    def __init__(self) -> None:
        self.exchanges: dict[str, FakeExchange] = {}
        self.queues: dict[str, FakeQueue] = {}
        self.exchange_calls: list[tuple[str, ExchangeType, bool]] = []
        self.queue_calls: list[tuple[str, bool, dict[str, object] | None]] = []

    async def declare_exchange(
        self,
        name: str,
        type: ExchangeType,
        *,
        durable: bool,
    ) -> FakeExchange:
        self.exchange_calls.append((name, type, durable))
        exchange = FakeExchange(name)
        self.exchanges[name] = exchange
        return exchange

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        arguments: dict[str, object] | None = None,
    ) -> FakeQueue:
        self.queue_calls.append((name, durable, arguments))
        queue = FakeQueue(name)
        self.queues[name] = queue
        return queue


@pytest.mark.asyncio
async def test_rabbitmq_topology_declares_full_durable_dead_letter_topology() -> None:
    channel = FakeChannel()
    topology = RabbitMQTopology()

    exchange = await topology.declare(channel)

    assert exchange.name == JOBS_DIRECT_EXCHANGE_NAME
    assert channel.exchange_calls == [
        (JOBS_DIRECT_EXCHANGE_NAME, ExchangeType.DIRECT, True),
        (JOBS_DLX_EXCHANGE_NAME, ExchangeType.DIRECT, True),
    ]
    assert channel.queue_calls == [
        (
            JOBS_EXECUTE_QUEUE_NAME,
            True,
            {
                "x-dead-letter-exchange": JOBS_DLX_EXCHANGE_NAME,
                "x-dead-letter-routing-key": JOBS_DEAD_ROUTING_KEY,
            },
        ),
        (JOBS_DEAD_QUEUE_NAME, True, None),
    ]
    assert channel.queues[JOBS_EXECUTE_QUEUE_NAME].bind_calls == [
        (channel.exchanges[JOBS_DIRECT_EXCHANGE_NAME], JOBS_EXECUTE_ROUTING_KEY)
    ]
    assert channel.queues[JOBS_DEAD_QUEUE_NAME].bind_calls == [
        (channel.exchanges[JOBS_DLX_EXCHANGE_NAME], JOBS_DEAD_ROUTING_KEY)
    ]


@pytest.mark.asyncio
async def test_rabbitmq_dispatch_publisher_emits_required_persistent_message_metadata() -> None:
    exchange = FakeExchange(JOBS_DIRECT_EXCHANGE_NAME)
    publisher = RabbitMQDispatchPublisher(exchange)
    created_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    outbox_event = OutboxEvent(
        aggregate_id=uuid.uuid4(),
        event_type="job.dispatch",
        payload={},
        available_at=created_at,
        publish_attempts=0,
    )
    outbox_event.id = uuid.uuid4()
    message = DispatchOutboxMessage(
        event_id=outbox_event.id,
        job_id=uuid.uuid4(),
        kind="dispatch",
        created_at=created_at,
    )

    await publisher.publish_dispatch(outbox_event=outbox_event, message=message)

    published_message, routing_key = exchange.published[0]
    assert routing_key == JOBS_EXECUTE_ROUTING_KEY
    assert published_message.delivery_mode == DeliveryMode.PERSISTENT
    assert published_message.content_type == "application/json"
    assert published_message.message_id == str(outbox_event.id)
    assert published_message.correlation_id == str(message.job_id)
    assert published_message.timestamp == created_at
    assert published_message.type == message.kind
    assert published_message.headers == {
        "kind": message.kind,
        "event_type": outbox_event.event_type,
    }
    assert json.loads(published_message.body.decode("utf-8")) == message.model_dump(mode="json")
