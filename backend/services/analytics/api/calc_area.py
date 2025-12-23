"""
Area calculation service (placeholder).
"""

from shapely.geometry import shape
from pyproj import Geod
import logging

geod = Geod(ellps="WGS84")
logger = logging.getLogger("landos.analytics")


def calculate_area_hectares(geometry: dict) -> float:
    geom = shape(geometry)
    if geom.is_empty:
        raise ValueError("geometry is empty")
    if not geom.is_valid:
        geom = geom.buffer(0)
    if geom.geom_type == "Polygon":
        geoms = [geom]
    else:
        geoms = list(geom.geoms)
    total_area = 0.0
    for g in geoms:
        lon, lat = g.exterior.coords.xy
        area, _ = geod.polygon_area_perimeter(lon, lat)
        total_area += abs(area)
    hectares = total_area / 10_000.0
    logger.info("Computed area %.2f ha (geom_type=%s, parts=%d)", hectares, geom.geom_type, len(geoms))
    return hectares


async def compute_area_hectares(geometry: dict) -> float:
    return calculate_area_hectares(geometry)
