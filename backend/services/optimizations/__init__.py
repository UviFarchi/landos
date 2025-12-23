"""Optimizations engine router and initialization."""

from fastapi import APIRouter

from backend.services.optimizations import api

router = APIRouter(prefix="/api/optimizations", tags=["optimizations"])


@router.get("/ping")
async def ping():
    return await api.ping()


async def initialize():
    """
    Initialize optimizations engine (placeholder).
    """
    return await api.initialize()
