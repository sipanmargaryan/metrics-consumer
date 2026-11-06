from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from app.config import settings
from app.db.postgres import AsyncSessionLocal, database_lifespan
from app.helpers.kafka import KafkaWorker
from app.helpers.logging import configure_logging
from app.routers.health import router as health_router
from app.routers.orders.services.orders import OrderService

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    async with database_lifespan():
        # Fail startup if migrations have not been applied; do not consume first.
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("SELECT message_id FROM received_orders LIMIT 0")
            )
        service = OrderService(AsyncSessionLocal)
        worker = KafkaWorker(settings.kafka, service.handle)
        app.state.worker = worker
        await worker.start()
        try:
            yield
        finally:
            await worker.stop()


app = FastAPI(title="Metrics Consumer", version="0.1.0", lifespan=lifespan)
app.include_router(health_router)
