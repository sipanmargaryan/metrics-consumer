from typing import Literal

from pydantic import UUID7, AwareDatetime, BaseModel, Field


class OrderCreated(BaseModel):
    order_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)
    price: int = Field(ge=0)
    order_type: Literal["created"]
    created_at: AwareDatetime


class OrderMessage(BaseModel):
    message_id: UUID7
    data: OrderCreated
