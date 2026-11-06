import logging

from aiokafka import ConsumerRecord
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.routers.orders.repository import OrderRepository
from app.routers.orders.schemas import OrderMessage

logger = logging.getLogger(__name__)


class OrderService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def handle(self, record: ConsumerRecord) -> None:
        message = OrderMessage.model_validate_json(record.value)
        async with self.sessions.begin() as session:
            inserted = await OrderRepository(session).record(message, record)
            # Add database business changes here only when inserted is True.
            # They must share this transaction with the duplicate guard.
        logger.info(
            "%s order event %s for order %s",
            "Recorded" if inserted else "Skipped duplicate",
            message.message_id,
            message.data.order_id,
        )
