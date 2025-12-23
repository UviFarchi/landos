"""
Soil data ETL.

Fetch SSURGO soil attributes for a project polygon and store map unit records.
"""

import datetime
import httpx
from shapely.geometry import shape
from shapely import wkt
import logging
import numpy as np
from rasterio.features import rasterize

from backend.services.analytics.analytics_db_connection import analytics_db

SSURGO_URL = "https://sdmdataaccess.nrcs.usda.gov/Tabular/post.rest"
logger = logging.getLogger("landos.analytics")
SOIL_TIMEOUT = 180.0  # allow larger polygons to complete


def _normalize_geometry(geometry: dict):
    if geometry.get("type") == "FeatureCollection":
        features = geometry.get("features") or []
        if not features:
            raise ValueError("FeatureCollection is empty")
        geometry = features[0].get("geometry") or {}
    if geometry.get("type") == "Feature":
        geometry = geometry.get("geometry") or {}
    return geometry


async def fetch_soil_data(project: dict):
    if analytics_db.client is None:
        analytics_db.connect()
    db = analytics_db.get_db()

    project_id = project.get("project_id")
    geometry = project.get("geometry")
    if not project_id or not geometry:
        raise ValueError("project_id and geometry are required for soil ETL")

    geometry = _normalize_geometry(geometry)
    logger.info("Soil ETL fetching for project %s", project_id)
    geom = shape(geometry)
    geom_wkt_text = geom.wkt
    columns = [
        "mukey",
        "muname",
        "cokey",
        "compname",
        "comppct_r",
        "drainagecl",
        "depth_top",
        "depth_bottom",
        "ph",
        "organic_matter",
        "water_capacity",
        "sand",
        "clay",
        "wkt",
    ]
    query = f"""
        SELECT mu.mukey, mu.muname, c.cokey, c.compname, c.comppct_r, c.drainagecl, ch.hzdept_r, ch.hzdepb_r, ch.ph1to1h2o_r, ch.om_r, ch.awc_r, ch.sandtotal_r, ch.claytotal_r, mp.mupolygongeo.STAsText()
        FROM mupolygon mp INNER JOIN mapunit mu ON mu.mukey = mp.mukey INNER JOIN component c ON c.mukey = mu.mukey LEFT JOIN chorizon ch ON ch.cokey = c.cokey
        WHERE c.majcompflag = 'Yes' AND mp.mupolygongeo.STIntersects(geometry::STGeomFromText('{geom_wkt_text}', 4326)) = 1
        ORDER BY mu.mukey, c.cokey, ch.hzdept_r
    """
    async with httpx.AsyncClient(timeout=SOIL_TIMEOUT) as client:
        attempts = 0
        last_exc = None
        while attempts < 2:
            attempts += 1
            try:
                resp = await client.post(SSURGO_URL, json={"query": query, "format": "JSON"})
                resp.raise_for_status()
                rows = resp.json().get("Table", [])
                break
            except Exception as exc:
                last_exc = exc
                logger.warning("Soil ETL attempt %s failed for project %s: %s", attempts, project_id, exc)
                if attempts >= 2:
                    raise
                await asyncio.sleep(1)
        mapped_rows = [dict(zip(columns, row)) for row in rows]
        logger.info("Soil ETL fetched %d rows for project %s", len(mapped_rows), project_id)

    terrain = await db.terrain.find_one({"project_id": project_id})
    if not terrain or not terrain.get("elevation_data"):
        raise RuntimeError("DEM must be loaded before soil ETL")
    elev = terrain["elevation_data"]
    heightmap = elev.get("heightmap") or []
    rows = len(heightmap)
    cols = len(heightmap[0]) if rows else 0
    transform = elev.get("transform")
    if not rows or not cols or not transform:
        raise RuntimeError("DEM heightmap and transform are required for soil rasterization")

    mukey_to_id = {}
    id_to_mukey = {}
    unit_attrs = {}
    shapes = []
    for row in mapped_rows:
        mukey = str(row.get("mukey") or "").strip()
        poly_wkt = row.get("wkt")
        if not mukey or not poly_wkt:
            continue
        try:
            poly_geom = wkt.loads(poly_wkt)
            if not poly_geom.is_valid:
                poly_geom = poly_geom.buffer(0)
        except Exception:
            continue
        idx = mukey_to_id.setdefault(mukey, len(mukey_to_id) + 1)
        id_to_mukey[str(idx)] = mukey
        attrs = {k: v for k, v in row.items() if k != "wkt"}
        unit_attrs[mukey] = attrs
        shapes.append((poly_geom, idx))

    soil_grid = None
    etl_status = {"status": "ok", "updated_at": datetime.datetime.utcnow().isoformat()}
    if not shapes:
        logger.warning("Soil ETL found no polygons to rasterize for project %s", project_id)
        etl_status = {"status": "failed", "error": "no soil polygons", "updated_at": datetime.datetime.utcnow().isoformat()}
    else:
        try:
            arr = rasterize(
                shapes,
                out_shape=(rows, cols),
                transform=transform,
                fill=0,
                dtype="int32",
                all_touched=False,
            )
            soil_grid = arr.astype(int).tolist()
        except Exception as exc:
            logger.exception("Soil rasterize failed for project %s: %s", project_id, exc)
            etl_status = {"status": "failed", "error": str(exc), "updated_at": datetime.datetime.utcnow().isoformat()}
            soil_grid = None

    soil_doc = {
        "source": "USDA_SSURGO",
        "map_units": mapped_rows,
        "units": unit_attrs,
        "index_map": id_to_mukey,
        "grid": soil_grid,
        "bounds": elev.get("bounds"),
        "transform": transform,
        "fetched_at": datetime.datetime.utcnow(),
    }

    await db.terrain.update_one(
        {"project_id": project_id},
        {"$set": {"soil_data": soil_doc, "etl_layers.soil": etl_status}},
        upsert=True,
    )
    logger.info("Soil ETL stored for project %s", project_id)
    return {"ok": True, "count": len(mapped_rows)}
