"""Analytics engine router and initialization."""

from fastapi import APIRouter

from backend.services.analytics import api, terrain
from backend.services.analytics.analytics_db_connection import analytics_db

compute_area_hectares = api.compute_area_hectares
resolve_region = api.resolve_region
validate_geometry = api.validate_geometry
trigger_etl = api.trigger_etl

router = APIRouter(prefix="/api/analytics", tags=["analytics"])

# Expose external services as routes based on config
if api.EXTERNAL_SERVICES.get("ping"):
    @router.get("/ping")
    async def ping():
        return await api.ping()


async def initialize():
    """
    Initialize analytics engine.
    """
    return await api.initialize()

# Export DB for internal callers (platform grid endpoint)
db = analytics_db
terrain = terrain
