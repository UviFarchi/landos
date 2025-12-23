"""
Configuration for the analytics service (URLs, db names, auth keys, etc.).
"""

import os

# Services marked external will be exposed over HTTP by the platform/router.
EXTERNAL_SERVICES = {
    "ping": True,
    "compute_area_hectares": False,
    "resolve_region": False,
    "trigger_etl": False,
}

MONGO_URL = os.getenv("ANALYTICS_MONGO_URL", "mongodb://localhost:27017")
MONGO_DB = os.getenv("ANALYTICS_DB_NAME", "analytics")

# Data sources (from landos_ref)
COUNTRIES_URL = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
SUBDIVISION_SOURCES = {
    "USA": {
        "level": "county",
        "source": "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_county_20m.zip",
    },
}

# DEM source
OPENTOPO_API_KEY = os.getenv("OPENTOPO_API_KEY", "8890d11a205337023a515bce10979bae")
