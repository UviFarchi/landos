"""Operations engine router and initialization."""

from fastapi import APIRouter

from backend.services.operations import api

router = APIRouter(prefix="/api/operations", tags=["operations"])


@router.get("/ping")
async def ping():
    return await api.ping()


async def initialize():
    """
    Initialize operations engine (placeholder).
    """
    return await api.initialize()
