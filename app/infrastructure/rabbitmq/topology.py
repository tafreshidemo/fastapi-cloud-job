from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol, cast

from aio_pika import DeliveryMode, ExchangeType, Message

from app.models.outbox_event import OutboxEvent
from app.schemas.outbox import DispatchOutboxMessage

JOBS_DIRECT_EXCHANGE_NAME = "jobs.direct"
JOBS_EXECUTE_QUEUE_NAME = "jobs.execute"
JOBS_EXECUTE_ROUTING_KEY = "jobs.execute"
JOBS_DLX_EXCHANGE_NAME = "jobs.dlx"
JOBS_DEAD_QUEUE_NAME = "jobs.dead"
JOBS_DEAD_ROUTING_KEY = "jobs.dead"


def execute_queue_arguments() -> dict[str, Any]:
    return {
        "x-dead-letter-exchange": JOBS_DLX_EXCHANGE_NAME,
        "x-dead-letter-routing-key": JOBS_DEAD_ROUTING_KEY,
    }


class RabbitMQExchange(Protocol):
    def publish(self, message: Message, routing_key: str) -> Awaitable[Any]: ...


class RabbitMQQueue(Protocol):
    def bind(self, exchange: RabbitMQExchange, routing_key: str) -> Awaitable[Any]: ...


class RabbitMQChannel(Protocol):
    def declare_exchange(
        self,
        name: str,
        type: ExchangeType,
        *,
        durable: bool,
    ) -> Awaitable[RabbitMQExchange]: ...

    def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        arguments: dict[str, Any] | None = None,
    ) -> Awaitable[RabbitMQQueue]: ...


class RabbitMQTopology:
    async def declare(self, channel: Any) -> RabbitMQExchange:
        main_exchange = cast(
            RabbitMQExchange,
            await channel.declare_exchange(
                JOBS_DIRECT_EXCHANGE_NAME,
                ExchangeType.DIRECT,
                durable=True,
            ),
        )
        dead_letter_exchange = cast(
            RabbitMQExchange,
            await channel.declare_exchange(
                JOBS_DLX_EXCHANGE_NAME,
                ExchangeType.DIRECT,
                durable=True,
            ),
        )
        execute_queue = cast(
            RabbitMQQueue,
            await channel.declare_queue(
                JOBS_EXECUTE_QUEUE_NAME,
                durable=True,
                arguments=execute_queue_arguments(),
            ),
        )
        dead_letter_queue = cast(
            RabbitMQQueue,
            await channel.declare_queue(
                JOBS_DEAD_QUEUE_NAME,
                durable=True,
            ),
        )
        await execute_queue.bind(main_exchange, routing_key=JOBS_EXECUTE_ROUTING_KEY)
        await dead_letter_queue.bind(dead_letter_exchange, routing_key=JOBS_DEAD_ROUTING_KEY)
        return main_exchange


class RabbitMQDispatchPublisher:
    def __init__(self, exchange: RabbitMQExchange) -> None:
        self._exchange = exchange

    async def publish_dispatch(
        self,
        *,
        outbox_event: OutboxEvent,
        message: DispatchOutboxMessage,
    ) -> None:
        await self._exchange.publish(
            Message(
                body=message.model_dump_json().encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=str(outbox_event.id),
                correlation_id=str(message.job_id),
                timestamp=message.created_at,
                type=message.kind,
                headers={
                    "kind": message.kind,
                    "event_type": outbox_event.event_type,
                },
            ),
            routing_key=JOBS_EXECUTE_ROUTING_KEY,
        )
