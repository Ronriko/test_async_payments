from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_session
from app.schemas.payments import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/health/ready")
async def readiness(session: Annotated[AsyncSession, Depends(get_session)]) -> HealthResponse:
    await session.execute(text("SELECT 1"))
    return HealthResponse(status="ok")
