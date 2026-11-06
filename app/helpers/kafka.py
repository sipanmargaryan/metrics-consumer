import asyncio
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition

from app.config import KafkaSettings

logger = logging.getLogger(__name__)
type MessageHandler = Callable[[ConsumerRecord], Awaitable[None]]


async def process_record(
    consumer: AIOKafkaConsumer, record: ConsumerRecord, handler: MessageHandler
) -> None:
    # Never commit the consumer's entire fetched position: commit only this record.
    await handler(record)
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
            try:
                await consumer.start()
                self.ready = True
                async for record in consumer:
                    try:
                        async with asyncio.timeout(
                            self.settings.processing_timeout_seconds
                        ):
                            await process_record(consumer, record, self.handler)
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
                try:
                    await consumer.stop()
                except Exception as exc:
                    logger.error("Consumer cleanup failed: %s", type(exc).__name__)
            # Rejoin from committed offsets, rather than skipping a failed record.
            await asyncio.sleep(self.settings.retry_backoff_seconds)
