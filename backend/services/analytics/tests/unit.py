"""Analytics unit tests."""

import json
from pathlib import Path

import pytest
from shapely.geometry import Polygon, shape

from backend.services.analytics import api
import backend.services.analytics.api.soil as soil
import backend.services.analytics.api.trigger_etl as etl
import httpx
import numpy as np
import rasterio
from rasterio.io import MemoryFile


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


def test_square_bbox_adds_buffer_and_is_square():
    import importlib
    trigger_mod = importlib.import_module("backend.services.analytics.api.trigger_etl")
    geom = Polygon([(0, 0), (2, 0), (2, 1), (0, 1)])
    minx, miny, maxx, maxy = trigger_mod._square_bbox(geom, buffer_ratio=0.1, min_buffer_deg=0.002)
    side = maxx - minx
    span = max(2, 1)
    expected = max(span * 1.1, span + 0.002, 0.002)
    assert side == pytest.approx(maxy - miny), "BBox should be square"
    assert side >= expected, "BBox should respect buffer ratio/min buffer"


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


@pytest.mark.anyio
async def test_fetch_tiff_uses_bbox_cache(monkeypatch, tmp_path):
    import importlib
    trigger_mod = importlib.import_module("backend.services.analytics.api.trigger_etl")
    geom = _load_sample("valid_small.json")["features"][0]["geometry"]
    bbox = trigger_mod._square_bbox(shape(geom))
    suffix = f"{bbox[0]:.4f}_{bbox[1]:.4f}_{bbox[2]:.4f}_{bbox[3]:.4f}.tif".replace(".", "_").replace("-", "m")
    cached_path = tmp_path / f"cached_{suffix}"
    cached_bytes = b"cached"
    cached_path.write_bytes(cached_bytes)
    monkeypatch.setattr(trigger_mod, "CACHE_DIR", tmp_path)

    class NeverClient:
        async def __aenter__(self):
            raise AssertionError("Should not hit network when cache exists")
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False
    monkeypatch.setattr(trigger_mod.httpx, "AsyncClient", lambda *args, **kwargs: NeverClient())

    content = await trigger_mod._fetch_tiff(geom, "proj-cache")
    assert content == cached_bytes, "Should return cached DEM when bbox key matches"


@pytest.mark.anyio
async def test_soil_etl_records_status_and_grid(monkeypatch):
    # Mock SSURGO response with a simple polygon and DEM in terrain
    class FakeResp:
        def __init__(self, table):
            self._table = table
        def raise_for_status(self): return None
        def json(self): return {"Table": self._table}
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return False
        async def post(self, *args, **kwargs):
            # Return a single square polygon at index 1
            poly_wkt = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
            return FakeResp([[1, "Unit", "c1", "Comp", 100, "well", 0, 1, 7, 2, 0.2, 50, 20, poly_wkt]])
    class FakeCollection:
        def __init__(self):
            self.docs = {"p1": {"elevation_data": {"heightmap": [[1, 2], [3, 4]], "transform": [1, 0, 0, 0, -1, 2]}}}
        async def find_one(self, filt):
            return self.docs.get(filt.get("project_id"))
        async def update_one(self, filt, update, upsert=False):
            doc = self.docs.setdefault(filt.get("project_id"), {})
            for k, v in (update.get("$set", {}) or {}).items():
                if "." in k:
                    top, sub = k.split(".", 1)
                    doc.setdefault(top, {})[sub] = v
                else:
                    doc[k] = v
    class FakeDB:
        def __init__(self):
            self.terrain = FakeCollection()
    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDB()
        def connect(self): self.client = True
        def get_db(self): return self.db
    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(soil, "analytics_db", fake_db)
    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=None: FakeClient())
    result = await soil.fetch_soil_data({"project_id": "p1", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]} })
    assert result.get("ok") is True
    terrain = await fake_db.db.terrain.find_one({"project_id": "p1"})
    soil_doc = terrain.get("soil_data")
    assert soil_doc and soil_doc.get("grid"), "Soil grid should be rasterized"
    status = terrain.get("etl_layers", {}).get("soil")
    assert status and status.get("status") == "ok", "Soil ETL status should be ok"


@pytest.mark.anyio
async def test_soil_etl_records_failure_when_no_polygons(monkeypatch):
    class FakeResp:
        def raise_for_status(self): return None
        def json(self): return {"Table": []}
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return False
        async def post(self, *args, **kwargs): return FakeResp()
    class FakeCollection:
        def __init__(self):
            self.docs = {"p_empty": {"elevation_data": {"heightmap": [[1, 2], [3, 4]], "transform": [1, 0, 0, 0, -1, 2]}}}
        async def find_one(self, filt):
            return self.docs.get(filt.get("project_id"))
        async def update_one(self, filt, update, upsert=False):
            doc = self.docs.setdefault(filt.get("project_id"), {})
            for k, v in (update.get("$set", {}) or {}).items():
                if "." in k:
                    top, sub = k.split(".", 1)
                    doc.setdefault(top, {})[sub] = v
                else:
                    doc[k] = v
    class FakeDB:
        def __init__(self):
            self.terrain = FakeCollection()
    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDB()
        def connect(self): self.client = True
        def get_db(self): return self.db
    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(soil, "analytics_db", fake_db)
    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=None: FakeClient())
    await soil.fetch_soil_data({"project_id": "p_empty", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]} })
    terrain = await fake_db.db.terrain.find_one({"project_id": "p_empty"})
    status = terrain.get("etl_layers", {}).get("soil")
    assert status and status.get("status") == "failed", "Should record failure when no soil polygons"


@pytest.mark.anyio
async def test_soil_etl_retries_once_on_failure(monkeypatch):
    calls = {"count": 0}
    class FakeResp:
        def __init__(self, table):
            self._table = table
        def raise_for_status(self): return None
        def json(self): return {"Table": self._table}
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return False
        async def post(self, *args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise httpx.ConnectTimeout("boom")
            poly_wkt = "POLYGON((0 0,1 0,1 1,0 1,0 0))"
            return FakeResp([[1, "Unit", "c1", "Comp", 100, "well", 0, 1, 7, 2, 0.2, 50, 20, poly_wkt]])
    class FakeCollection:
        def __init__(self):
            self.docs = {"p_retry": {"elevation_data": {"heightmap": [[1, 2], [3, 4]], "transform": [1, 0, 0, 0, -1, 2]}}}
        async def find_one(self, filt):
            return self.docs.get(filt.get("project_id"))
        async def update_one(self, filt, update, upsert=False):
            doc = self.docs.setdefault(filt.get("project_id"), {})
            for k, v in (update.get("$set", {}) or {}).items():
                if "." in k:
                    top, sub = k.split(".", 1)
                    doc.setdefault(top, {})[sub] = v
                else:
                    doc[k] = v
    class FakeDB:
        def __init__(self):
            self.terrain = FakeCollection()
    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDB()
        def connect(self): self.client = True
        def get_db(self): return self.db
    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(soil, "analytics_db", fake_db)
    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=None: FakeClient())
    await soil.fetch_soil_data({"project_id": "p_retry", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]} })
    assert calls["count"] == 2, "Should retry once after initial failure"
    terrain = await fake_db.db.terrain.find_one({"project_id": "p_retry"})
    status = terrain.get("etl_layers", {}).get("soil")
    assert status and status.get("status") == "ok"

@pytest.mark.anyio
async def test_soil_etl_records_failure_on_raster_error(monkeypatch):
    # Make rasterize raise and ensure status is failure
    class FakeResp:
        def raise_for_status(self): return None
        def json(self): return {"Table": [[1, "Unit", "c1", "Comp", 100, "well", 0, 1, 7, 2, 0.2, 50, 20, "POLYGON((0 0,1 0,1 1,0 1,0 0))"]]}
    class FakeClient:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc_val, exc_tb): return False
        async def post(self, *args, **kwargs): return FakeResp()
    class FakeCollection:
        def __init__(self):
            self.docs = {"p2": {"elevation_data": {"heightmap": [[1, 2], [3, 4]], "transform": [1, 0, 0, 0, -1, 2]}}}
        async def find_one(self, filt):
            return self.docs.get(filt.get("project_id"))
        async def update_one(self, filt, update, upsert=False):
            doc = self.docs.setdefault(filt.get("project_id"), {})
            for k, v in (update.get("$set", {}) or {}).items():
                if "." in k:
                    top, sub = k.split(".", 1)
                    doc.setdefault(top, {})[sub] = v
                else:
                    doc[k] = v
    class FakeDB:
        def __init__(self):
            self.terrain = FakeCollection()
    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDB()
        def connect(self): self.client = True
        def get_db(self): return self.db
    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(soil, "analytics_db", fake_db)
    monkeypatch.setattr(httpx, "AsyncClient", lambda timeout=None: FakeClient())
    monkeypatch.setattr(soil, "rasterize", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("fail")))
    await soil.fetch_soil_data({"project_id": "p2", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]} })
    terrain = await fake_db.db.terrain.find_one({"project_id": "p2"})
    status = terrain.get("etl_layers", {}).get("soil")
    assert status and status.get("status") == "failed", "Soil ETL status should record failure"


@pytest.mark.anyio
async def test_process_tiff_pads_to_square_and_sets_dem_status(monkeypatch):
    # Build a 1x2 GeoTIFF in memory
    data = np.array([[1, 2]], dtype=np.int16)
    meta = {
        "driver": "GTiff",
        "dtype": "int16",
        "count": 1,
        "height": data.shape[0],
        "width": data.shape[1],
        "transform": rasterio.transform.from_origin(0, 1, 1, 1),
    }
    with MemoryFile() as memfile:
        with memfile.open(**meta) as dataset:
            dataset.write(data, 1)
        tiff_bytes = memfile.read()
    import importlib
    trigger_mod = importlib.import_module("backend.services.analytics.api.trigger_etl")
    padded = trigger_mod._process_tiff(tiff_bytes)
    assert len(padded["heightmap"]) == len(padded["heightmap"][0]) == 2, "Heightmap should be square padded"
    # trigger_etl sets etl_layers.dem; reuse fake DB and bypass soil
    class FakeTerrain:
        def __init__(self):
            self.docs = {}
        async def update_one(self, filt, update, upsert=False):
            doc = self.docs.setdefault(filt.get("project_id"), {})
            doc.update(update.get("$set", {}))
    class FakeDBInner:
        def __init__(self):
            self.terrain = FakeTerrain()
            self.projects = FakeTerrain()
        def get_db(self): return self
    class FakeAnalyticsDB:
        def __init__(self):
            self.client = True
            self.db = FakeDBInner()
        def connect(self): self.client = True
        def get_db(self): return self.db
    fake_db = FakeAnalyticsDB()
    monkeypatch.setattr(trigger_mod, "analytics_db", fake_db)
    async def fake_fetch_tiff(geom, pid):
        return tiff_bytes
    monkeypatch.setattr(trigger_mod, "_fetch_tiff", fake_fetch_tiff)
    monkeypatch.setattr(trigger_mod, "soil", soil)
    async def fake_soil(project):
        return None
    monkeypatch.setattr(soil, "fetch_soil_data", fake_soil)
    res = await trigger_mod.trigger_etl({"project_id": "sq1", "geometry": {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]} })
    assert res.get("ok") is True
    stored = fake_db.db.terrain.docs.get("sq1")
    assert stored and stored.get("etl_layers", {}).get("dem", {}).get("status") == "ok"
