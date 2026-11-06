from aiokafka import ConsumerRecord
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.routers.orders.models import ReceivedOrder
from app.routers.orders.schemas import OrderMessage


class OrderRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record(self, message: OrderMessage, record: ConsumerRecord) -> bool:
        data = message.data
        statement = (
            insert(ReceivedOrder)
            .values(
                message_id=message.message_id,
                order_id=data.order_id,
                product_id=data.product_id,
                quantity=data.quantity,
                price=data.price,
                order_type=data.order_type,
                order_created_at=data.created_at,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
            )
            .on_conflict_do_nothing(index_elements=[ReceivedOrder.message_id])
            .returning(ReceivedOrder.message_id)
        )
        result = await self.session.execute(statement)
        return result.scalar_one_or_none() is not None
