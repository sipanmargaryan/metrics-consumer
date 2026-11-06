from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import text

from app.db.postgres import AsyncSessionLocal

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/ready")
async def ready(request: Request) -> dict[str, str]:
    if not request.app.state.worker.is_ready():
        raise HTTPException(status_code=503, detail="Consumer is not ready")
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        raise HTTPException(status_code=503, detail="Database is not ready") from None
    return {"status": "ready"}
