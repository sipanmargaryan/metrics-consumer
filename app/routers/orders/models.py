from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.db.postgres import BaseDBModel


class ReceivedOrder(BaseDBModel):
    __tablename__ = "received_orders"

    message_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    order_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    product_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    quantity: Mapped[int] = mapped_column(BigInteger, nullable=False)
    price: Mapped[int] = mapped_column(BigInteger, nullable=False)
    order_type: Mapped[str] = mapped_column(String(32), nullable=False)
    order_created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    topic: Mapped[str] = mapped_column(String(249), nullable=False)
    partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
