"""
ETL trigger service.

Fetch DEM data for a project using OpenTopography SRTM (90m) and store the
heightmap and stats in Mongo. If a cached GeoTIFF exists on disk, reuse it.
"""

from datetime import datetime
from pathlib import Path
import logging
import httpx
import rasterio
from rasterio.io import MemoryFile
import numpy as np
from shapely.geometry import shape

from backend.services.analytics.analytics_db_connection import analytics_db
import backend.services.analytics.api.soil as soil
from backend.services.analytics import config

OPENTOPO_URL = "https://portal.opentopography.org/API/globaldem"
OPENTOPO_DEM = "SRTMGL3"
OPENTOPO_TIMEOUT = 180.0
CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "dem_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("landos.analytics")


def _geometry_from_geojson(geometry: dict):
    if geometry.get("type") == "Feature":
        return geometry["geometry"]
    if geometry.get("type") == "FeatureCollection":
        features = geometry.get("features") or []
        if not features:
            raise ValueError("FeatureCollection is empty")
        return features[0]["geometry"]
    return geometry


def _square_bbox(geom_shape, buffer_ratio=0.1, min_buffer_deg=0.002):
    """Return a square bbox (minx, miny, maxx, maxy) that encloses the geometry with buffer."""
    minx, miny, maxx, maxy = geom_shape.bounds
    cx = (minx + maxx) / 2
    cy = (miny + maxy) / 2
    span = max(maxx - minx, maxy - miny)
    buffered_span = max(span * (1 + buffer_ratio), span + min_buffer_deg, min_buffer_deg)
    half = buffered_span / 2
    return cx - half, cy - half, cx + half, cy + half


async def _fetch_tiff(geom, project_id: str):
    geom_shape = shape(geom)
    minx, miny, maxx, maxy = _square_bbox(geom_shape)
    params = {
        "demtype": OPENTOPO_DEM,
        "south": miny,
        "north": maxy,
        "west": minx,
        "east": maxx,
        "outputFormat": "GTiff",
        "API_Key": config.OPENTOPO_API_KEY,
    }
    bbox_key = f"{minx:.4f}_{miny:.4f}_{maxx:.4f}_{maxy:.4f}.tif".replace(".", "_").replace("-", "m")
    existing = list(CACHE_DIR.glob(f"*_{bbox_key}"))
    if existing:
        cache_path = existing[0]
        logger.info("DEM cache hit for project %s -> %s", project_id, cache_path.name)
        return cache_path.read_bytes()
    cache_path = CACHE_DIR / f"dem_{bbox_key}"
    logger.info("DEM fetch start for project %s (url=%s)", project_id, OPENTOPO_URL)
    async with httpx.AsyncClient() as client:
        resp = await client.get(OPENTOPO_URL, params=params, timeout=OPENTOPO_TIMEOUT)
        resp.raise_for_status()
        cache_path.write_bytes(resp.content)
        logger.info("DEM fetched for project %s, saved %s", project_id, cache_path.name)
        return resp.content


def _process_tiff(tiff_bytes: bytes):
    with MemoryFile(tiff_bytes) as memfile:
        with memfile.open() as dataset:
            elevation_array = dataset.read(1)
            nodata = dataset.nodata
            if nodata is not None:
                elevation_array = np.where(elevation_array == nodata, 0, elevation_array)
            # ensure square grid (pad/crop to max dimension)
            height, width = elevation_array.shape
            target = max(height, width)
            if height != width:
                padded = np.zeros((target, target), dtype=elevation_array.dtype)
                r0 = (target - height) // 2
                c0 = (target - width) // 2
                padded[r0:r0 + height, c0:c0 + width] = elevation_array
                elevation_array = padded
            min_elev = float(np.min(elevation_array))
            max_elev = float(np.max(elevation_array))
            heightmap = elevation_array.tolist()
            bounds = {
                "left": dataset.bounds.left,
                "bottom": dataset.bounds.bottom,
                "right": dataset.bounds.right,
                "top": dataset.bounds.top,
            }
            transform = list(dataset.transform)
    logger.info("DEM processed (min=%.2f, max=%.2f)", min_elev, max_elev)
    return {
        "heightmap": heightmap,
        "min_elevation": min_elev,
        "max_elevation": max_elev,
        "bounds": bounds,
        "transform": transform,
        "resolution": OPENTOPO_DEM,
        "source": OPENTOPO_URL,
    }


async def trigger_etl(project: dict):
    """
    Fetch and store DEM data for the project.
    """
    if analytics_db.client is None:
        analytics_db.connect()
    db = analytics_db.get_db()

    project_id = project.get("project_id")
    geometry = project.get("geometry")
    if not project_id or not geometry:
        raise ValueError("project_id and geometry are required for ETL")

    logger.info("ETL start for project %s", project_id)
    geom = _geometry_from_geojson(geometry)
    tiff_bytes = await _fetch_tiff(geom, project_id)
    elevation = _process_tiff(tiff_bytes)
    elevation["fetched_at"] = datetime.utcnow()

    terrain_doc = {
        "project_id": project_id,
        "elevation_data": elevation,
        "etl_layers": {
            "dem": {
                "status": "ok",
                "updated_at": datetime.utcnow().isoformat(),
            }
        },
    }

    await db.terrain.update_one({"project_id": project_id}, {"$set": terrain_doc}, upsert=True)
    await db.projects.update_one({"project_id": project_id}, {"$set": {"status": "dem_loaded"}})
    logger.info("DEM stored for project %s", project_id)
    try:
        logger.info("Soil ETL start for project %s", project_id)
        await soil.fetch_soil_data({"project_id": project_id, "geometry": geom})
        logger.info("Soil ETL finished for project %s", project_id)
    except Exception as exc:
        # Surface errors to caller so tests can catch missing implementation
        logger.exception("Soil ETL failed for project %s: %s", project_id, exc)
        await db.terrain.update_one(
            {"project_id": project_id},
            {
                "$set": {
                    "etl_layers.soil": {
                        "status": "failed",
                        "error": str(exc),
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                }
            },
            upsert=True,
        )
        raise
    logger.info("ETL completed for project %s", project_id)
    return {"ok": True}
