"""USA-specific ETL orchestration."""

import logging

from backend.services.analytics.terrain.usa import soil, land_cover

logger = logging.getLogger("landos.analytics")


async def initialize(db):
    """
    Prepare USA datasets (e.g., land cover keys).
    """
    await land_cover.load_land_cover_keys(db)


async def run_all(project: dict):
    """
    Run all USA layers (soil + land cover).
    """
    await soil.fetch_soil_data(project)
    await land_cover.fetch_land_cover_data(project)


async def run_layer(layer: str, project: dict):
    lname = (layer or "").lower()
    if lname == "soil":
        return await soil.fetch_soil_data(project)
    if lname in {"land_cover", "landcover", "land-cover"}:
        return await land_cover.fetch_land_cover_data(project)
    raise RuntimeError(f"Unsupported USA layer ETL: {layer}")
