"""
Country-specific ETL dispatch.
"""

import logging
from typing import Optional, Callable, Awaitable

from backend.services.analytics.terrain import usa

logger = logging.getLogger("landos.analytics")

COUNTRY_MODULES = {
    "USA": usa,
}


def _get_country_module(country_code: Optional[str]):
    code = (country_code or "").upper()
    if code in COUNTRY_MODULES:
        return COUNTRY_MODULES[code]
    raise RuntimeError(f"No ETL module defined for country {code or '(unknown)'}")


async def run_country_etl(country_code: Optional[str], project: dict):
    """
    Run full country-specific ETL (all layers beyond DEM).
    """
    mod = _get_country_module(country_code)
    return await mod.run_all(project)


async def run_country_layer(country_code: Optional[str], layer: str, project: dict):
    """
    Run a specific layer ETL for the given country.
    """
    mod = _get_country_module(country_code)
    return await mod.run_layer(layer, project)


async def initialize_country(country_code: Optional[str], db):
    """
    Run country-specific dataset initialization (if defined).
    """
    mod = _get_country_module(country_code)
    if hasattr(mod, "initialize"):
        return await mod.initialize(db)
    return None


async def initialize_configured(db, countries: Optional[list] = None):
    """
    Initialize datasets for configured countries.
    """
    targets = countries if countries is not None else list(COUNTRY_MODULES.keys())
    for code in targets:
        try:
            await initialize_country(code, db)
        except Exception:
            logger.exception("Country initialization failed for %s", code)
