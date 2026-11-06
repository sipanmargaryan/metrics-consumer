"""Preserve rejected messages before acknowledging their source offsets.

Delivery to the DLQ is at least once: a crash between publish and source commit
can produce another copy. The stable source-coordinate key identifies duplicates.
Provision the DLQ with cleanup.policy=delete and max.message.bytes=2097152 to
allow room for metadata on top of the producer's roughly 1 MiB record limit.
"""

import json
import logging
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer, ConsumerRecord
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class InvalidMessageError(ValueError):
    """A source record cannot be interpreted as a supported application event."""


class DeadLetterQueue:
    def __init__(
        self, producer: AIOKafkaProducer, topic: str, consumer_group: str
    ) -> None:
        self.producer = producer
        self.topic = topic
        self.consumer_group = consumer_group

    async def publish(
        self, record: ConsumerRecord, error: ValidationError | InvalidMessageError
    ) -> None:
        details = []
        if isinstance(error, ValidationError):
            # Keep diagnostic locations/types without input values or exception context.
            details = [
                {"field": item["loc"], "type": item["type"]}
                for item in error.errors(
                    include_input=False, include_url=False, include_context=False
                )[:20]
            ]
        else:
            details = [{"type": "unsupported_tombstone"}]
        metadata = {
            "dlq.source.topic": record.topic,
            "dlq.source.partition": str(record.partition),
            "dlq.source.offset": str(record.offset),
            "dlq.source.timestamp": str(record.timestamp),
            "dlq.consumer.group": self.consumer_group,
            "dlq.failed_at": datetime.now(UTC).isoformat(),
            "dlq.error.type": type(error).__name__,
            "dlq.error.details": json.dumps(details),
        }
        headers = list(record.headers or [])
        headers.extend((key, value.encode()) for key, value in metadata.items())
        headers.append(("dlq.source.key", record.key))
        # Preserve original bytes, including non-JSON messages and tombstones.
        # The deterministic key also survives a source offset-commit failure.
        key = f"{record.topic}:{record.partition}:{record.offset}".encode()
        await self.producer.send_and_wait(
            self.topic, key=key, value=record.value, headers=headers
        )
        logger.warning(
            "Message preserved in dead-letter topic",
            extra={
                "dead_letter_topic": self.topic,
                "source_topic": record.topic,
                "source_partition": record.partition,
                "source_offset": record.offset,
                "error_type": type(error).__name__,
            },
        )
