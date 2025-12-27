"""
Analytics engine API and service registry.

Internal callers and external routes should use these functions. External services
are determined by config.EXTERNAL_SERVICES.
"""

import os
import zipfile
from datetime import datetime
from pathlib import Path
import logging
import httpx
import shapefile
from shapely.geometry import shape, mapping, Polygon, MultiPolygon

from backend.services.analytics import config
from backend.services.analytics.analytics_db_connection import analytics_db
from backend.services.analytics.api import ping as ping_service
from backend.services.analytics.api import calc_area
from backend.services.analytics.api import determine_region
from backend.services.analytics.api import trigger_etl as etl_service
from backend.services.analytics import terrain
from backend.services.analytics import scheduler
from pymongo.errors import BulkWriteError

EXTERNAL_SERVICES = getattr(config, "EXTERNAL_SERVICES", {})
logger = logging.getLogger("landos.analytics")

# Re-export service functions for callers/monkeypatching
ping = ping_service.ping
resolve_region = determine_region.resolve_region
trigger_etl = etl_service.trigger_etl


async def compute_area_hectares(geometry: dict) -> float:
    cleaned = validate_geometry(geometry)
    return await calc_area.compute_area_hectares(cleaned)

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def validate_geometry(geometry: dict) -> dict:
    if not isinstance(geometry, dict):
        raise ValueError("geometry must be a GeoJSON object")
    gtype = geometry.get("type")
    if gtype == "FeatureCollection":
        features = geometry.get("features") or []
        if not features:
            raise ValueError("geometry FeatureCollection is empty")
        geometry = features[0].get("geometry") or {}
        gtype = geometry.get("type")
    if gtype == "Feature":
        geometry = geometry.get("geometry") or {}
        gtype = geometry.get("type")
    if gtype not in {"Polygon", "MultiPolygon"}:
        raise ValueError("geometry must be Polygon or MultiPolygon")
    coords = geometry.get("coordinates")
    if not coords:
        raise ValueError("geometry coordinates are required")
    logger.info("Geometry validated (type=%s)", gtype)
    return geometry


def calculate_area_hectares(geometry: dict) -> float:
    cleaned = validate_geometry(geometry)
    area = calc_area.calculate_area_hectares(cleaned)
    logger.info("Area calculated: %.2f ha", area)
    return area


async def _download_if_missing(url: str, dest: Path):
    if dest.exists():
        logger.info("Using cached download for %s", dest)
        return dest
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=30)
            resp.raise_for_status()
            dest.write_bytes(resp.content)
        logger.info("Downloaded %s to %s", url, dest)
        return dest
    except Exception as e:
        logger.error("Download failed for %s: %s", url, e)
        raise


def _extract_zip(zip_path: Path, target_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)
    return target_dir


def _load_shapefile_records(shp_dir: Path):
    shp_files = list(shp_dir.glob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp file found in {shp_dir}")
    reader = shapefile.Reader(shp_files[0])
    fields = [f[0] for f in reader.fields[1:]]
    for sr in reader.iterShapeRecords():
        attrs = dict(zip(fields, sr.record))
        geom = shape(sr.shape.__geo_interface__)
        if geom.is_empty:
            continue
        if not geom.is_valid:
            geom = geom.buffer(0)
        if isinstance(geom, Polygon):
            geom = MultiPolygon([geom])
        yield attrs, mapping(geom)


async def _ensure_countries(db):
    count = await db.regions.count_documents({})
    if count > 0:
        logger.info("Countries already loaded (%s)", count)
        return
    zip_path = DATA_DIR / "countries.zip"
    await _download_if_missing(config.COUNTRIES_URL, zip_path)
    docs = []
    extract_dir = DATA_DIR / "countries"
    _extract_zip(zip_path, extract_dir)
    for attrs, geom in _load_shapefile_records(extract_dir):
        code = attrs.get("ADM0_A3") or attrs.get("ISO_A3") or attrs.get("ISO_A3_EH") or "UNK"
        name = attrs.get("NAME") or attrs.get("ADMIN") or "Unknown"
        docs.append(
            {
                "type": "country",
                "code": code,
                "name": name,
                "geometry": geom,
            }
        )
    try:
        await db.regions.insert_many(docs, ordered=False)
    except BulkWriteError:
        for doc in docs:
            try:
                await db.regions.insert_one(doc)
            except Exception:
                continue
    logger.info("Countries loaded: %s", len(docs))
    await db.datasets.update_one(
        {"name": "countries"},
        {"$set": scheduler.build_dataset_metadata("countries", datetime.utcnow())},
        upsert=True,
    )
    await db.refresh_jobs.update_one(
        {"name": "countries"},
        {"$set": scheduler.build_refresh_job("countries", 365)},
        upsert=True,
    )
    logger.info("Countries dataset metadata registered")


async def _ensure_subdivisions(db):
    total = await db.subdivisions.count_documents({})
    if total > 0:
        logger.info("Subdivisions already loaded (%s)", total)
        return
    for country_code, meta in config.SUBDIVISION_SOURCES.items():
        url = meta.get("source")
        if not url:
            continue
        zip_path = DATA_DIR / f"subdivisions_{country_code}.zip"
        await _download_if_missing(url, zip_path)
        docs = []
        extract_dir = DATA_DIR / f"subdivisions_{country_code}"
        _extract_zip(zip_path, extract_dir)
        for attrs, geom in _load_shapefile_records(extract_dir):
            code = attrs.get("GEOID") or attrs.get("AFFGEOID") or attrs.get("ID") or "UNK"
            name = attrs.get("NAME") or attrs.get("NAMELSAD") or "Unknown"
            docs.append(
                {
                    "type": "subdivision",
                    "country": country_code,
                    "code": code,
                    "name": name,
                    "geometry": geom,
                }
            )
        if docs:
            try:
                await db.subdivisions.insert_many(docs, ordered=False)
            except BulkWriteError:
                # Fall back to best-effort insert
                for doc in docs:
                    try:
                        await db.subdivisions.insert_one(doc)
                    except Exception:
                        continue
        logger.info("Subdivisions loaded for %s: %s", country_code, len(docs))
        dataset_name = f"subdivisions_{country_code}"
        await db.datasets.update_one(
            {"name": dataset_name},
            {"$set": scheduler.build_dataset_metadata(dataset_name, datetime.utcnow())},
            upsert=True,
        )
        await db.refresh_jobs.update_one(
            {"name": dataset_name},
            {"$set": scheduler.build_refresh_job(dataset_name, 365)},
            upsert=True,
        )
        logger.info("Subdivision dataset metadata registered for %s", country_code)


async def initialize():
    """
    Initialize analytics engine: load countries, subdivisions, indexes, and scheduler entries.
    """
    logger.info("Analytics initialize: starting")
    analytics_db.mongo_url = os.getenv("ANALYTICS_MONGO_URL", config.MONGO_URL)
    analytics_db.db_name = os.getenv("ANALYTICS_DB_NAME", config.MONGO_DB)
    if analytics_db.client:
        analytics_db.close()
    logger.info("Connecting analytics DB at %s / %s", analytics_db.mongo_url, analytics_db.db_name)
    analytics_db.connect()
    db = analytics_db.get_db()

    await db.regions.create_index([("geometry", "2dsphere")])
    await db.subdivisions.create_index([("geometry", "2dsphere")])
    logger.info("Analytics DB connected (%s/%s); indexes ensured", analytics_db.mongo_url, analytics_db.db_name)

    await _ensure_countries(db)
    await _ensure_subdivisions(db)
    countries = list(getattr(config, "SUBDIVISION_SOURCES", {}).keys()) or list(terrain.COUNTRY_MODULES.keys())
    await terrain.initialize_configured(db, countries)

    logger.info("Analytics initialize: completed")
    return None
