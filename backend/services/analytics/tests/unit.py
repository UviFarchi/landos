"""Analytics unit tests."""

import json
from pathlib import Path

import pytest

from backend.services.analytics import api


def _load_sample(name: str) -> dict:
    samples_dir = Path(__file__).resolve().parents[3] / "tests" / "samples"
    with (samples_dir / name).open() as f:
        return json.load(f)


def test_external_services_registry():
    assert api.EXTERNAL_SERVICES.get("ping") is True, "Ping should be exposed externally"
    assert api.EXTERNAL_SERVICES.get("compute_area_hectares") is False, "Area compute should be internal"
    assert api.EXTERNAL_SERVICES.get("resolve_region") is False, "Region resolution should be internal"


def test_geometry_validation_rejects_invalid_geojson():
    with pytest.raises(ValueError):
        api.validate_geometry({"type": "Point"})
    with pytest.raises(ValueError):
        api.validate_geometry({"type": "Polygon", "coordinates": []})


def test_geometry_validation_accepts_valid_polygon():
    geom = _load_sample("valid_small.json")
    cleaned = api.validate_geometry(geom)
    assert cleaned.get("type") in {"Polygon", "MultiPolygon"}, "Valid geometry should be normalized"


def test_area_calculation_returns_numeric():
    geom = _load_sample("valid_small.json")
    area = api.calculate_area_hectares(geom)
    assert isinstance(area, (int, float))
    assert area > 0


# --- Scheduler helpers ---


def test_dataset_metadata_has_future_expiry(monkeypatch):
    from datetime import datetime, timedelta
    from backend.services.analytics import scheduler

    fixed_now = datetime(2023, 1, 1)
    monkeypatch.setattr(scheduler, "utcnow", lambda: fixed_now)

    meta = scheduler.build_dataset_metadata(name="countries", downloaded_at=fixed_now)
    assert meta["name"] == "countries"
    assert meta["downloaded_at"] == fixed_now
    assert meta["expires_at"] == fixed_now + timedelta(days=365)


def test_refresh_job_has_next_run(monkeypatch):
    from datetime import datetime, timedelta
    from backend.services.analytics import scheduler

    fixed_now = datetime(2023, 1, 1)
    monkeypatch.setattr(scheduler, "utcnow", lambda: fixed_now)

    job = scheduler.build_refresh_job(name="countries", interval_days=365)
    assert job["name"] == "countries"
    assert job["next_run_at"] == fixed_now + timedelta(days=365)


# --- ETL trigger ---


@pytest.mark.anyio
async def test_trigger_etl_exists_and_is_async(monkeypatch):
    import importlib
    etl = importlib.import_module("backend.services.analytics.api.trigger_etl")
    import backend.services.analytics.api.soil as soil
    geom = _load_sample("valid_small.json")

    # Fake raster bytes (2x2 GeoTIFF)
    def make_tiff_bytes():
        import numpy as np
        import rasterio
        from rasterio.io import MemoryFile
        data = np.array([[1, 2], [3, 4]], dtype=np.int16)
        meta = {
            "driver": "GTiff",
            "dtype": "int16",
            "count": 1,
            "height": data.shape[0],
            "width": data.shape[1],
            "transform": rasterio.transform.from_origin(0, 2, 1, 1),
        }
        with MemoryFile() as memfile:
            with memfile.open(**meta) as dataset:
                dataset.write(data, 1)
            return memfile.read()

    async def fake_fetch(geom, project_id):
        return make_tiff_bytes()
    soil_called = {}
    async def fake_soil(project):
        soil_called["called"] = True

    class FakeCollection:
        def __init__(self):
            self.docs = {}
        async def update_one(self, filter, update, upsert=False):
            doc = update.get("$set", {})
            key = filter.get("project_id")
            self.docs[key] = doc
        async def find_one(self, filter):
            return self.docs.get(filter.get("project_id"))

    class FakeDB:
        def __init__(self):
            self.terrain = FakeCollection()
            self.projects = FakeCollection()

    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDB()
        def connect(self):
            self.client = True
        def get_db(self):
            return self.db

    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(etl, "_fetch_tiff", fake_fetch)
    monkeypatch.setattr(etl, "analytics_db", fake_db)
    monkeypatch.setattr(etl, "soil", soil)
    monkeypatch.setattr(soil, "fetch_soil_data", fake_soil)

    result = await api.trigger_etl({"project_id": "p1", "geometry": geom})
    assert result.get("ok") is True
    assert fake_db.db.terrain.docs.get("p1"), "Terrain doc should be stored"
    assert soil_called.get("called"), "Soil ETL should be invoked after DEM"
