"""
Region resolution service (placeholder).
"""

import os
import logging
from backend.services.analytics.analytics_db_connection import analytics_db
from backend.services.analytics import config

logger = logging.getLogger("landos.analytics")


async def resolve_region(geometry: dict) -> dict:
    from backend.services.analytics.api import validate_geometry  # avoid circular import
    geometry = validate_geometry(geometry)
    logger.info("Resolving region for geometry")
    if analytics_db.client is None:
        analytics_db.mongo_url = os.getenv("ANALYTICS_MONGO_URL", config.MONGO_URL)
        analytics_db.db_name = os.getenv("ANALYTICS_DB_NAME", config.MONGO_DB)
        analytics_db.connect()
    db = analytics_db.get_db()
    country = await db.regions.find_one(
        {"geometry": {"$geoIntersects": {"$geometry": geometry}}},
        {"_id": 0, "code": 1, "name": 1},
    )
    subdivision = await db.subdivisions.find_one(
        {"geometry": {"$geoIntersects": {"$geometry": geometry}}},
        {"_id": 0, "code": 1, "name": 1},
    )
    result = {
        "country": country.get("code") if country else None,
        "country_name": country.get("name") if country else None,
        "subdivision": subdivision.get("code") if subdivision else None,
        "subdivision_name": subdivision.get("name") if subdivision else None,
    }
    logger.info(
        "Region resolved country=%s (%s) subdivision=%s (%s)",
        result["country"],
        result["country_name"],
        result["subdivision"],
        result["subdivision_name"],
    )
    return result
