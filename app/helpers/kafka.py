import asyncio
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from pydantic import ValidationError

from app.config import KafkaSettings
from app.helpers.dead_letter import DeadLetterQueue, InvalidMessageError

logger = logging.getLogger(__name__)
type MessageHandler = Callable[[ConsumerRecord], Awaitable[None]]


async def process_record(
    consumer: AIOKafkaConsumer,
    record: ConsumerRecord,
    handler: MessageHandler,
    dead_letters: DeadLetterQueue | None = None,
) -> None:
    # Never commit the consumer's entire fetched position: commit only this record.
    try:
        if record.value is None:
            raise InvalidMessageError("Order events cannot be tombstones")
        await handler(record)
    except (ValidationError, InvalidMessageError) as exc:
        if dead_letters is None:
            raise
        # send_and_wait must succeed before the source offset can advance.
        await dead_letters.publish(record, exc)
    await consumer.commit(
        {TopicPartition(record.topic, record.partition): record.offset + 1}
    )


class KafkaWorker:
    def __init__(self, settings: KafkaSettings, handler: MessageHandler) -> None:
        self.settings = settings
        self.handler = handler
        self.ready = False
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self.run(), name="orders-consumer")

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self.ready = False

    def is_ready(self) -> bool:
        return self.ready and self.task is not None and not self.task.done()

    async def run(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                self.settings.topic,
                bootstrap_servers=self.settings.bootstrap_servers,
                group_id=self.settings.group_id,
                enable_auto_commit=False,
                auto_offset_reset=self.settings.auto_offset_reset,
                isolation_level="read_committed",
                max_poll_interval_ms=300_000,
                request_timeout_ms=30_000,
            )
            producer = AIOKafkaProducer(
                bootstrap_servers=self.settings.bootstrap_servers,
                enable_idempotence=True,
                acks="all",
                max_request_size=2 * 1024 * 1024,
            )
            dead_letters = DeadLetterQueue(
                producer, self.settings.dead_letter_topic, self.settings.group_id
            )
            try:
                await producer.start()
                await consumer.start()
                self.ready = True
                async for record in consumer:
                    try:
                        async with asyncio.timeout(
                            self.settings.processing_timeout_seconds
                        ):
                            await process_record(
                                consumer, record, self.handler, dead_letters
                            )
                    except Exception:
                        # Do not log payloads or validation errors containing input.
                        logger.error(
                            "Record failed; offset remains uncommitted: %s/%s/%s",
                            record.topic,
                            record.partition,
                            record.offset,
                        )
                        raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Consumer restarting after %s", type(exc).__name__)
            finally:
                self.ready = False
                for resource in (consumer, producer):
                    try:
                        await resource.stop()
                    except Exception as exc:
                        logger.error("Kafka cleanup failed: %s", type(exc).__name__)
            # Rejoin from committed offsets, rather than skipping a failed record.
            await asyncio.sleep(self.settings.retry_backoff_seconds)
