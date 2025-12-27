"""
Analytics integration tests (real Mongo, real services).

These tests will remain red until the analytics engine implements initialization,
area computation, and region resolution.
"""

import json
from pathlib import Path
from datetime import datetime

import pytest
from motor.motor_asyncio import AsyncIOMotorClient

from backend.services.analytics import api
import os


def _load_sample(name: str) -> dict:
    samples_dir = Path(__file__).resolve().parents[3] / "tests" / "samples"
    with (samples_dir / name).open() as f:
        return json.load(f)


@pytest.fixture
async def analytics_env(monkeypatch):
    mongo_url = "mongodb://localhost:27017"
    db_name = "analytics_integration"
    monkeypatch.setenv("ANALYTICS_MONGO_URL", mongo_url)
    monkeypatch.setenv("ANALYTICS_DB_NAME", db_name)
    client = AsyncIOMotorClient(mongo_url)
    await client.drop_database(db_name)
    yield mongo_url, db_name
    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_initialize_loads_regions_and_index(analytics_env):
    """
    Analytics initialize should populate regions and ensure a 2dsphere index.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    await api.initialize()

    count = await db.regions.count_documents({})
    assert count > 0, "Regions collection should be populated after init"

    indexes = await db.regions.index_information()
    assert any(idx.get("key") == [("geometry", "2dsphere")] for idx in indexes.values()), "2dsphere index required"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_compute_area_returns_hectares(analytics_env):
    """
    Area computation should return a numeric value in hectares for a valid polygon.
    """
    cfg_url, db_name = analytics_env
    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    geometry = _load_sample("valid_small.json")
    area = await api.compute_area_hectares(geometry)
    assert isinstance(area, (int, float)), "Area should be numeric"
    assert area > 0, "Area should be positive"


@pytest.mark.integration
@pytest.mark.anyio
async def test_resolve_region_returns_country_and_subdivision(analytics_env):
    """
    Region resolution should return country and subdivision codes for a valid polygon.
    """
    cfg_url, db_name = analytics_env
    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    geometry = _load_sample("valid_small.json")
    region = await api.resolve_region(geometry)
    assert region.get("country"), "Country code should be returned"
    assert region.get("subdivision"), "Subdivision code should be returned"
    assert region.get("country_name"), "Country name should be returned"
    assert region.get("subdivision_name"), "Subdivision name should be returned"

    client = AsyncIOMotorClient(cfg_url)
    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_countries_loaded_and_dataset_registered(analytics_env):
    """
    Countries should be loaded into the DB and registered with a refresh expiry (VHS/cached downloads preferred).
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    countries = await db.regions.count_documents({})
    assert countries > 0, "Countries should be loaded into regions collection"

    meta = await db.datasets.find_one({"name": "countries"})
    assert meta is not None, "Dataset metadata for countries should be registered"
    assert meta.get("expires_at"), "Dataset should have an expiry/refresh date"
    expires = meta["expires_at"]
    assert expires > datetime.utcnow(), "Expiry should be in the future"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_subdivisions_loaded_per_config(analytics_env):
    """
    Configured subdivisions (e.g., USA) should be loaded and registered for refresh.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    subdivisions_count = await db.subdivisions.count_documents({"country": "USA"})
    assert subdivisions_count > 0, "Configured subdivisions should be loaded into the shared collection"

    meta = await db.datasets.find_one({"name": "subdivisions_USA"})
    assert meta is not None, "Subdivision dataset should be registered"
    assert meta.get("expires_at"), "Subdivision dataset should have an expiry"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_scheduler_tracks_refresh_jobs(analytics_env):
    """
    Scheduler should track refresh jobs for loaded datasets (countries/subdivisions).
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    jobs = await db.refresh_jobs.count_documents({})
    assert jobs >= 2, "Refresh jobs should exist for countries and subdivisions"

    sample_job = await db.refresh_jobs.find_one({})
    assert sample_job.get("name"), "Refresh job should have a name"
    assert sample_job.get("next_run_at"), "Refresh job should have next_run_at"
    assert sample_job["next_run_at"] > datetime.utcnow(), "next_run_at should be in the future"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_dem_etl_fetches_and_stores_heightmap(analytics_env, vhs):
    """
    DEM ETL should fetch elevation tiles and store heightmap and stats for the project.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    # Seed a project with valid geometry
    project_id = "proj_dem_1"
    geometry = _load_sample("valid_small.json")
    await db.projects.insert_one({"project_id": project_id, "geometry": geometry})

    # Trigger DEM ETL (expected to download SRTM via OpenTopography)
    async with vhs("dem_etl"):
        await api.trigger_etl({"project_id": project_id, "geometry": geometry})

    terrain = await db.terrain.find_one({"project_id": project_id})
    assert terrain is not None, "Terrain doc should be created"
    elevation = terrain.get("elevation_data") or {}
    assert elevation.get("heightmap"), "Heightmap should be stored"
    assert elevation.get("min_elevation") is not None, "Min elevation should be recorded"
    assert elevation.get("max_elevation") is not None, "Max elevation should be recorded"
    hm = elevation.get("heightmap") or []
    if hm:
        assert len(hm) > 0 and len(hm[0]) > 0, "Heightmap should have rows and cols"
    assert elevation.get("bounds"), "DEM bounds should be recorded"
    assert elevation.get("transform"), "DEM transform should be recorded"
    assert elevation.get("resolution"), "DEM resolution should be recorded"
    assert elevation.get("source"), "DEM source URL should be recorded"
    dem_status = terrain.get("etl_layers", {}).get("dem")
    assert dem_status and dem_status.get("status") == "ok", "DEM ETL status should be tracked"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_soil_etl_fetches_and_stores_map_units(analytics_env, vhs):
    """
    Soil ETL should fetch soil map units and align them to the project grid.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    project_id = "proj_soil_1"
    geometry = _load_sample("valid_small.json")
    await db.projects.insert_one({"project_id": project_id, "geometry": geometry})

    async with vhs("soil_etl_status"):
        await api.trigger_etl({"project_id": project_id, "geometry": geometry})

    terrain = await db.terrain.find_one({"project_id": project_id})
    assert terrain is not None, "Terrain doc should be created"
    soil = terrain.get("soil_data") or {}
    assert soil.get("map_units"), "Soil map units should be stored"
    assert soil.get("grid"), "Soil grid should be rasterized"
    status = terrain.get("etl_layers", {}).get("soil")
    assert status and status.get("status") == "ok", "Soil ETL status should be recorded"
    dem_status = terrain.get("etl_layers", {}).get("dem")
    assert dem_status and dem_status.get("status") == "ok", "DEM ETL status should remain ok"
    elev = terrain.get("elevation_data") or {}
    hm = elev.get("heightmap") or []
    if hm:
        assert len(soil["grid"]) == len(hm)
        assert len(soil["grid"][0]) == len(hm[0])

    await client.drop_database(db_name)
    client.close()

# Land cover integration tests (to be implemented with land_cover module)


@pytest.mark.integration
@pytest.mark.anyio
async def test_land_cover_init_loads_keys_and_metadata(analytics_env, vhs):
    """
    Analytics initialize should load land cover keys if missing and register refresh job.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url

    async with vhs("land_cover_init"):
        await api.initialize()

    keys = await db.land_cover_keys.count_documents({})
    assert keys > 0, "Land cover keys should be loaded"
    meta = await db.datasets.find_one({"name": "land_cover_keys"})
    assert meta and meta.get("expires_at"), "Land cover dataset metadata should be registered"
    refresh = await db.refresh_jobs.find_one({"name": "land_cover_keys"})
    assert refresh and refresh.get("next_run_at"), "Land cover refresh job should be scheduled"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_land_cover_etl_fetches_and_stores_grid(analytics_env, vhs):
    """
    Land cover ETL should rasterize codes and attach names from keys.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    project_id = "proj_landcover"
    geometry = _load_sample("valid_small.json")
    await db.projects.insert_one({"project_id": project_id, "geometry": geometry})

    async with vhs("land_cover_etl"):
        await api.trigger_etl({"project_id": project_id, "geometry": geometry})

    terrain = await db.terrain.find_one({"project_id": project_id})
    assert terrain is not None
    lc = terrain.get("land_cover") or {}
    assert lc.get("grid")
    assert lc.get("index_map")
    lc_status = terrain.get("etl_layers", {}).get("land_cover")
    assert lc_status and lc_status.get("status") == "ok"

    await client.drop_database(db_name)
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_land_cover_etl_failure_then_success_on_retry(analytics_env, vhs):
    """
    Land cover ETL should mark status failed on error, then succeed on retry.
    """
    cfg_url, db_name = analytics_env
    client = AsyncIOMotorClient(cfg_url)
    db = client[db_name]

    os.environ["ANALYTICS_DB_NAME"] = db_name
    os.environ["ANALYTICS_MONGO_URL"] = cfg_url
    await api.initialize()

    project_id = "proj_landcover_retry"
    geometry = _load_sample("valid_small.json")
    await db.projects.insert_one({"project_id": project_id, "geometry": geometry})

    os.environ["LAND_COVER_REMOTE_ONLY"] = "1"
    os.environ["LAND_COVER_FORCE_ERROR_ONCE"] = "1"
    with pytest.raises(RuntimeError):
        async with vhs("land_cover_etl_fail"):
            await api.trigger_etl({"project_id": project_id, "geometry": geometry})
    os.environ.pop("LAND_COVER_REMOTE_ONLY", None)
    os.environ.pop("LAND_COVER_FORCE_ERROR_ONCE", None)
    terrain = await db.terrain.find_one({"project_id": project_id})
    lc_status = terrain.get("etl_layers", {}).get("land_cover")
    assert lc_status and lc_status.get("status") == "failed"

    async with vhs("land_cover_etl"):
        await api.trigger_etl({"project_id": project_id, "geometry": geometry})
    terrain = await db.terrain.find_one({"project_id": project_id})
    lc_status = terrain.get("etl_layers", {}).get("land_cover")
    assert lc_status and lc_status.get("status") == "ok"
    assert terrain.get("land_cover", {}).get("grid")

    await client.drop_database(db_name)
    client.close()
