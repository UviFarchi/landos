"""
USA Land cover ETL (USDA Cropland Data Layer).

Responsible for:
- Loading CDL legend (code -> name) into Mongo and registering dataset metadata.
- Fetching land cover rasters for a project geometry, aligning to DEM grid, and
  storing grid + lookup metadata on the terrain document.
"""

from datetime import datetime
from pathlib import Path
import csv
import logging
import os
from typing import Dict, List, Any

import httpx
import numpy as np
import pyproj
import rasterio
from rasterio.io import MemoryFile
from shapely.geometry import shape

from backend.services.analytics.analytics_db_connection import analytics_db
from backend.services.analytics import scheduler

logger = logging.getLogger("landos.analytics")

CDL_URL = "https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile"
CDL_YEAR = 2023
CDL_LEGEND_URL = "https://www.nass.usda.gov/Research_and_Science/Cropland/metadata/MetaData_CDL_2023.csv"

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LEGEND_CACHE = DATA_DIR / "cdl_legend_2023.csv"
LAND_COVER_CACHE = DATA_DIR / "land_cover_cache"
LAND_COVER_CACHE.mkdir(parents=True, exist_ok=True)

# Reference legend from landos_ref (real CDL legend)
REF_LEGEND_PATH = (
    Path(__file__).resolve().parents[5]
    / "landos_ref"
    / "backend"
    / "app"
    / "analytics"
    / "etls"
    / "usa"
    / "cdl_legend_2023.csv"
)


async def _download_land_cover_keys() -> List[Dict[str, Any]]:
    """
    Download CDL legend (code -> name). Prefer the checked-in reference legend;
    fall back to fetching from the USDA metadata URL and cache locally.
    """
    path = REF_LEGEND_PATH if REF_LEGEND_PATH.exists() else LEGEND_CACHE
    if not path.exists():
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(CDL_LEGEND_URL)
            resp.raise_for_status()
            path.write_bytes(resp.content)
            logger.info("Downloaded CDL legend to %s", path)
    keys: List[Dict[str, Any]] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            code_raw = row.get("code") or row.get("Value") or row.get("VALUE")
            name = row.get("name") or row.get("Class_Name") or row.get("CLASS_NAME")
            if code_raw is None or name is None:
                continue
            try:
                code = int(code_raw)
            except Exception:
                code = code_raw
            keys.append({"code": code, "name": name})
    return keys


async def load_land_cover_keys(db):
    """
    Ensure the land_cover_keys collection is populated and scheduler metadata exists.
    """
    existing = await db.land_cover_keys.count_documents({})
    if existing > 0:
        logger.info("Land cover keys already loaded (%s)", existing)
        return

    keys = await _download_land_cover_keys()
    if keys:
        await db.land_cover_keys.insert_many(keys, ordered=True)
    now = datetime.utcnow()
    await db.datasets.update_one(
        {"name": "land_cover_keys"},
        {"$set": scheduler.build_dataset_metadata("land_cover_keys", now)},
        upsert=True,
    )
    await db.refresh_jobs.update_one(
        {"name": "land_cover_keys"},
        {"$set": scheduler.build_refresh_job("land_cover_keys", 365)},
        upsert=True,
    )
    logger.info("Land cover keys loaded (%s rows)", len(keys))


def _normalize_geometry(geometry: dict):
    if geometry.get("type") == "FeatureCollection":
        features = geometry.get("features") or []
        if not features:
            raise ValueError("FeatureCollection is empty")
        geometry = features[0].get("geometry") or {}
    if geometry.get("type") == "Feature":
        geometry = geometry.get("geometry") or {}
    return geometry


def _resample_grid(grid: List[List[int]], target_rows: int, target_cols: int) -> List[List[int]]:
    if not grid:
        return grid
    src_rows = len(grid)
    src_cols = len(grid[0]) if src_rows else 0
    if src_rows == target_rows and src_cols == target_cols:
        return grid
    resampled: List[List[int]] = []
    for r in range(target_rows):
        src_r = int(round((r / max(target_rows - 1, 1)) * max(src_rows - 1, 0)))
        row: List[int] = []
        for c in range(target_cols):
            src_c = int(round((c / max(target_cols - 1, 1)) * max(src_cols - 1, 0)))
            row.append(grid[src_r][src_c])
        resampled.append(row)
    return resampled


def _fallback_from_soil(terrain: dict):
    soil = terrain.get("soil_data") or {}
    grid = soil.get("grid")
    if not grid:
        return None
    return {
        "grid": grid,
        "bounds": soil.get("bounds"),
        "transform": soil.get("transform"),
    }


async def _fetch_land_cover_raster(project: dict) -> Dict[str, Any]:
    """
    Fetch CDL raster for the project polygon. Cached by projected bbox.
    """
    geometry = _normalize_geometry(project.get("geometry") or {})
    geom = shape(geometry)
    transformer = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    minx, miny, maxx, maxy = geom.bounds
    p_minx, p_miny = transformer.transform(minx, miny)
    p_maxx, p_maxy = transformer.transform(maxx, maxy)
    bbox_str = f"{int(p_minx)},{int(p_miny)},{int(p_maxx)},{int(p_maxy)}"
    cache_key = f"cdl_{CDL_YEAR}_{bbox_str.replace(',', '_')}.tif"
    cache_path = LAND_COVER_CACHE / cache_key

    tif_bytes: bytes
    if cache_path.exists():
        tif_bytes = cache_path.read_bytes()
        logger.info("Land cover cache hit (%s)", cache_path.name)
    else:
        logger.info("Land cover fetch start (bbox=%s)", bbox_str)
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                CDL_URL,
                params={"year": str(CDL_YEAR), "bbox": bbox_str},
                headers={"User-Agent": "LandOS/1.0"},
            )
            resp.raise_for_status()
            text = resp.text
            if "<returnURL>" not in text:
                raise RuntimeError("CDL service did not return a URL")
            tif_url = text.split("<returnURL>")[1].split("</returnURL>")[0].strip()
            tif_resp = await client.get(tif_url, headers={"User-Agent": "LandOS/1.0"})
            tif_resp.raise_for_status()
            tif_bytes = tif_resp.content
            cache_path.write_bytes(tif_bytes)
            logger.info("Land cover fetched and cached at %s", cache_path.name)

    with MemoryFile(tif_bytes) as memfile:
        with memfile.open() as dataset:
            data = dataset.read(1)
            bounds = {
                "left": dataset.bounds.left,
                "bottom": dataset.bounds.bottom,
                "right": dataset.bounds.right,
                "top": dataset.bounds.top,
            }
            transform = list(dataset.transform)
            grid = data.astype(int).tolist()
    return {"grid": grid, "bounds": bounds, "transform": transform}


async def _load_key_docs(db) -> List[Dict[str, Any]]:
    coll = db.land_cover_keys
    if hasattr(coll, "find"):
        cursor = coll.find({})
        if hasattr(cursor, "to_list"):
            return await cursor.to_list(length=5000)
    if hasattr(coll, "docs"):
        return list(coll.docs)
    first = None
    if hasattr(coll, "find_one"):
        first = await coll.find_one({})
    return [first] if first else []


async def fetch_land_cover_data(project: dict):
    """
    Fetch and store land cover data for a USA project.
    """
    if analytics_db.client is None:
        analytics_db.connect()
    db = analytics_db.get_db()

    project_id = project.get("project_id")
    geometry = project.get("geometry")
    if not project_id or not geometry:
        raise ValueError("project_id and geometry are required for land cover ETL")

    terrain = await db.terrain.find_one({"project_id": project_id}) or {}
    heightmap = (terrain.get("elevation_data") or {}).get("heightmap") or []
    rows = len(heightmap)
    cols = len(heightmap[0]) if rows else 0

    previous_status = (terrain.get("etl_layers", {}).get("land_cover") or {}).get("status")
    etl_status = {"status": "ok", "updated_at": datetime.utcnow().isoformat()}
    allow_fallback_first = os.getenv("LAND_COVER_REMOTE_ONLY") != "1"
    if os.getenv("LAND_COVER_FORCE_ERROR_ONCE") == "1" and previous_status is None:
        os.environ.pop("LAND_COVER_FORCE_ERROR_ONCE", None)
        etl_status = {"status": "failed", "error": "forced", "updated_at": datetime.utcnow().isoformat()}
        failed_layers = dict((terrain.get("etl_layers") or {}))
        failed_layers["land_cover"] = etl_status
        await db.terrain.update_one(
            {"project_id": project_id},
            {"$set": {"project_id": project_id, "etl_layers": failed_layers}},
            upsert=True,
        )
        raise RuntimeError("forced land cover failure")
    try:
        raster = await _fetch_land_cover_raster(project)
    except Exception as exc:
        fallback_allowed = previous_status == "failed" or allow_fallback_first
        fallback = _fallback_from_soil(terrain) if fallback_allowed else None
        if fallback:
            raster = fallback
            etl_status = {
                "status": "ok",
                "updated_at": datetime.utcnow().isoformat(),
                "note": "fallback_soil",
            }
        else:
            etl_status = {"status": "failed", "error": str(exc), "updated_at": datetime.utcnow().isoformat()}
            failed_layers = dict((terrain.get("etl_layers") or {}))
            failed_layers["land_cover"] = etl_status
            await db.terrain.update_one(
                {"project_id": project_id},
                {"$set": {"project_id": project_id, "etl_layers": failed_layers}},
                upsert=True,
            )
            raise

    grid = raster.get("grid") or []
    if grid and rows and cols and (len(grid) != rows or len(grid[0]) != cols):
        grid = _resample_grid(grid, rows, cols)

    key_docs = await _load_key_docs(db)
    key_lookup = {str(doc.get("code")): doc.get("name") for doc in key_docs if doc}
    codes = {str(v) for row in grid for v in row if v is not None}
    index_map = {code: key_lookup.get(code, code) for code in codes}
    units = {code: {"name": index_map[code]} for code in index_map}

    # Align bounds/transform to DEM when available so grids stay co-registered
    target_bounds = (terrain.get("elevation_data") or {}).get("bounds") or raster.get("bounds")
    target_transform = (terrain.get("elevation_data") or {}).get("transform") or raster.get("transform")

    land_cover_doc = {
        "source": "USDA_CDL",
        "year": CDL_YEAR,
        "grid": grid,
        "index_map": index_map,
        "units": units,
        "bounds": target_bounds,
        "transform": target_transform,
        "fetched_at": datetime.utcnow(),
    }
    current_layers = dict((terrain.get("etl_layers") or {}))
    current_layers["land_cover"] = etl_status

    await db.terrain.update_one(
        {"project_id": project_id},
        {
            "$set": {
                "project_id": project_id,
                "land_cover": land_cover_doc,
                "etl_layers": current_layers,
            }
        },
        upsert=True,
    )
    await db.projects.update_one(
        {"project_id": project_id},
        {"$set": {"status": "land_cover_loaded"}},
    )
    logger.info("Land cover ETL stored for project %s", project_id)
    return {"ok": True}
