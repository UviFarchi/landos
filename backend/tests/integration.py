"""
Integration tests for the platform.

These exercises run the real app against a test Mongo DB to validate startup,
health, and core collection initialization. Additional journeys will be added
as endpoints are implemented.
"""

import json
from pathlib import Path

import pytest
from httpx import AsyncClient, ASGITransport
from motor.motor_asyncio import AsyncIOMotorClient

from backend import create_app
from backend.platform.config import PlatformConfig
from backend.platform.db_connection import PlatformDatabase


@pytest.fixture
async def platform_config():
    """
    Shared platform test DB; cleaned before and after each test.
    """
    cfg = PlatformConfig(
        mongo_url="mongodb://localhost:27017",
        mongo_db="platform_integration",
        auth_secret="dev-secret",
    )
    client = AsyncIOMotorClient(cfg.mongo_url)
    await client.drop_database(cfg.mongo_db)
    yield cfg
    await client.drop_database(cfg.mongo_db)
    client.close()


@pytest.fixture(autouse=True)
async def clean_analytics_db():
    """
    Ensure analytics DB is clean so ETL must run for each test.
    """
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    await client.drop_database("analytics")
    yield
    await client.drop_database("analytics")
    client.close()


@pytest.mark.integration
@pytest.mark.anyio
async def test_startup_health_and_collections(platform_config):
    """
    App should start, respond to /health, and ensure required collections exist.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200, "health endpoint should be available"
            assert resp.json() == {"status": "ok"}, "health endpoint should return ok status"

        collections = set(await db.get_db().list_collection_names())
        assert {"users", "projects"} <= collections, "Core collections should be ensured on startup"

    assert db.client is None, "DB client should be closed after shutdown"

    # Cleanup test database
@pytest.mark.integration
@pytest.mark.anyio
async def test_engine_routes_registered(platform_config):
    """
    Engine routes (analytics/operations/optimizations) should be mounted and respond.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            for engine, path in [
                ("analytics", "/api/analytics/ping"),
                ("operations", "/api/operations/ping"),
                ("optimizations", "/api/optimizations/ping"),
            ]:
                resp = await client.get(path)
                assert resp.status_code == 200, f"{engine} route should be registered"
                body = resp.json()
                assert body.get("engine") == engine, f"{engine} route should identify engine"
                assert body.get("ok") is True, f"{engine} route should signal ok"

    # Cleanup test database
@pytest.mark.integration
@pytest.mark.anyio
async def test_user_creation_persists_user_record(platform_config):
    """
    Creating a user with valid payload should persist the record and return success.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            payload = {"username": "alice", "password": "pw123"}
            resp = await client.post("/api/platform/signup", json=payload)
            assert resp.status_code == 201, "User creation should return 201"
            body = resp.json()
            assert body.get("ok") is True, "User creation response should confirm success"

        # Verify persistence
        user = await db.get_db().users.find_one({"username": "alice"})
        assert user is not None, "User record should be persisted"
        assert user.get("password"), "Password hash should be stored"

    # Cleanup test database
@pytest.mark.integration
@pytest.mark.anyio
async def test_login_failure_rejected(platform_config):
    """
    Login with wrong credentials should fail cleanly.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    from backend.platform import utils as validators
    # Seed a user directly
    db.connect()
    await db.get_db().users.insert_one(
        {
            "username": "alice",
            "password": validators.hash_password("pw123", cfg.auth_secret),
        }
    )
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/platform/login", json={"username": "alice", "password": "wrong"})
            assert resp.status_code in (400, 401), "Failed login should return client error"
            body = resp.json()
            assert body.get("ok") is False or body.get("error"), "Response should indicate failure"

@pytest.mark.integration
@pytest.mark.anyio
async def test_login_success_returns_ok(platform_config):
    """
    Login with correct credentials should succeed.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    from backend.platform import utils as validators
    # Seed a user directly
    db.connect()
    await db.get_db().users.insert_one(
        {
            "username": "bob",
            "password": validators.hash_password("pw123", cfg.auth_secret),
        }
    )
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/platform/login", json={"username": "bob", "password": "pw123"})
            assert resp.status_code == 200, "Successful login should return 200"
            body = resp.json()
            assert body.get("ok") is True, "Response should indicate success"

@pytest.mark.integration
@pytest.mark.anyio
async def test_engine_initializers_run_before_serving(platform_config):
    """
    Engine initializers must complete during startup before serving requests.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        assert getattr(app.state, "engines_ready", False) is True, "Engines should be marked ready after init"
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/health")
            assert resp.status_code == 200

    cleanup_client = AsyncIOMotorClient(cfg.mongo_url)
    await cleanup_client.drop_database(cfg.mongo_db)
    cleanup_client.close()


def _load_sample(path: str):
    sample_path = Path(__file__).resolve().parent / "samples" / path
    with sample_path.open() as f:
        return json.load(f)


@pytest.mark.integration
@pytest.mark.anyio
async def test_project_creation_rejects_invalid_area(platform_config):
    """
    Project creation should reject polygons that are too small or too large.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user
    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123"})
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            for sample in ["invalid_small.json", "invalid_large.json"]:
                payload = {
                    "username": "alice",
                    "name": "Bad Project",
                    "geometry": _load_sample(sample),
                }
                resp = await client.post("/api/platform/projects", json=payload)
                assert resp.status_code == 400, f"{sample} should be rejected"
                body = resp.json()
                assert body.get("error"), "Error message should be provided for invalid project area"

@pytest.mark.integration
@pytest.mark.anyio
async def test_project_creation_success_persists_and_links_user(platform_config, vhs):
    """
    Project creation should accept valid payload, return project_id, and persist/link records.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user
    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123"})
    db.close()

    transport = ASGITransport(app=app)
    async with vhs("platform_grid_etl"):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                payload = {
                    "username": "alice",
                    "name": "Valid Project",
                    "geometry": _load_sample("valid_small.json"),
                }
                resp = await client.post("/api/platform/projects", json=payload)
                assert resp.status_code == 201, "Valid project should be created"
                body = resp.json()
                project_id = body.get("project_id")
                assert project_id, "Response should include project_id"

            # Verify project persisted and user linked
            project = await db.get_db().projects.find_one({"project_id": project_id})
            assert project is not None, "Project should be stored in DB"
            assert project.get("username") == "alice", "Project should be linked to user"
            assert project.get("name") == "Valid Project", "Project name should be stored"
            assert project.get("geometry"), "Project geometry should be stored"
            assert project.get("created"), "Creation timestamp should be stored"
            assert project.get("country") and project.get("subdivision"), "Region codes should be stored"
            assert project.get("country_name") and project.get("subdivision_name"), "Region names should be stored"
            # Terrain should be generated by ETL
            analytics_client = AsyncIOMotorClient("mongodb://localhost:27017")
            terrain = await analytics_client.analytics.terrain.find_one({"project_id": project_id})
            assert terrain is not None, "Terrain should be stored by ETL after project creation"
            analytics_client.close()
            layers = terrain.get("etl_layers") or {}
            assert layers.get("dem", {}).get("status") == "ok"
            assert layers.get("soil", {}).get("status") == "ok"
            assert layers.get("land_cover", {}).get("status") == "ok"
            assert terrain.get("elevation_data") and terrain.get("soil_data") and terrain.get("land_cover")

            user = await db.get_db().users.find_one({"username": "alice"})
            linked = user.get("projects") or []
            assert project_id in linked, "User should be linked to the new project"

@pytest.mark.integration
@pytest.mark.anyio
async def test_project_listing_returns_user_projects(platform_config):
    """
    Listing projects for a user should return their projects.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user and projects directly
    db.connect()
    await db.get_db().users.insert_one({"username": "bob", "password": "pw123"})
    await db.get_db().projects.insert_many(
        [
            {"project_id": "p1", "username": "bob", "name": "Project One"},
            {"project_id": "p2", "username": "bob", "name": "Project Two"},
        ]
    )
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/api/platform/projects", json={"username": "bob"})
            assert resp.status_code == 200, "Project listing should succeed"
            body = resp.json()
            ids = {p.get("project_id") for p in body}
    assert {"p1", "p2"} <= ids, "Response should include all user projects"


@pytest.mark.integration
@pytest.mark.anyio
async def test_grid_endpoint_returns_layers(platform_config, vhs):
    """
    Grid endpoint should return DEM layer (and allow filtering by layer).
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user
    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123"})
    db.close()

    transport = ASGITransport(app=app)
    async with vhs("platform_grid_etl"):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                payload = {
                    "username": "alice",
                    "name": "Grid Project",
                    "geometry": _load_sample("valid_small.json"),
                }
                resp = await client.post("/api/platform/projects", json=payload)
                assert resp.status_code == 201
                project_id = resp.json().get("project_id")
                assert project_id

            # DEM should have been generated; query grid
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(f"/api/platform/projects/{project_id}/grid")
                assert resp.status_code == 200
                body = resp.json()
                assert body.get("layers", {}).get("dem"), "DEM layer should be present"
                assert body.get("layers", {}).get("soil"), "Soil layer should be present"
                assert body.get("layers", {}).get("land_cover"), "Land cover layer should be present"
                etl_layers = body.get("etl_layers") or {}
                assert etl_layers.get("dem", {}).get("status") == "ok"
                assert etl_layers.get("soil", {}).get("status") == "ok"
                assert etl_layers.get("land_cover", {}).get("status") == "ok"

                resp = await client.get(f"/api/platform/projects/{project_id}/grid", params={"layer": "dem"})
                assert resp.status_code == 200
                filtered = resp.json()
                assert filtered.get("layer") == "dem"
                assert filtered.get("data"), "Filtered DEM data should be present"
                assert filtered.get("etl_layers", {}).get("dem", {}).get("status") == "ok"
                resp = await client.get(f"/api/platform/projects/{project_id}/grid", params={"layer": "soil"})
                assert resp.status_code == 200
                filtered = resp.json()
                assert filtered.get("layer") == "soil"
                assert filtered.get("data"), "Filtered soil data should be present"
                assert filtered.get("etl_layers", {}).get("soil", {}).get("status") == "ok"
                resp = await client.get(f"/api/platform/projects/{project_id}/grid", params={"layer": "land_cover"})
                assert resp.status_code == 200
                filtered = resp.json()
                assert filtered.get("layer") == "land_cover"
                assert filtered.get("data"), "Filtered land cover data should be present"
                assert filtered.get("etl_layers", {}).get("land_cover", {}).get("status") == "ok"


@pytest.mark.integration
@pytest.mark.anyio
async def test_grid_endpoint_crops_to_polygon_with_buffer(platform_config, vhs):
    """
    Larger polygons should be stored cropped to the polygon bbox plus a 2-cell buffer, not padded to a square.
    """
    from shapely.geometry import shape

    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123"})
    db.close()

    geom = _load_sample("valid_large.json")
    poly = shape(geom["features"][0]["geometry"])
    minx, miny, maxx, maxy = poly.bounds

    transport = ASGITransport(app=app)
    async with vhs("platform_grid_etl_large"):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                payload = {"username": "alice", "name": "Grid Large Project", "geometry": geom}
                resp = await client.post("/api/platform/projects", json=payload)
                assert resp.status_code == 201
                project_id = resp.json().get("project_id")
                assert project_id

            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(f"/api/platform/projects/{project_id}/grid")
                assert resp.status_code == 200
                body = resp.json()
                layers = body.get("layers") or {}
                dem = layers.get("dem")
                soil = layers.get("soil")
                land_cover = layers.get("land_cover")
                assert dem and soil and land_cover, "DEM, soil, and land cover should all be present"
                hm = dem.get("heightmap") or []
                assert hm, "DEM heightmap should be present"
                rows = len(hm)
                cols = len(hm[0])
                assert soil.get("grid") and len(soil["grid"]) == rows and len(soil["grid"][0]) == cols, "Soil grid should align with DEM"
                assert land_cover.get("grid") and len(land_cover["grid"]) == rows and len(land_cover["grid"][0]) == cols, "Land cover grid should align with DEM"
                b = dem.get("bounds") or {}
                transform = dem.get("transform") or []
                assert transform and len(transform) >= 6, "DEM transform should be present"
                cell_w = abs(transform[0])
                cell_h = abs(transform[4])
                # Expect bbox cropped to polygon + 2 cells in every direction (within one cell tolerance)
                expected = {
                    "left": minx - 2 * cell_w,
                    "right": maxx + 2 * cell_w,
                    "bottom": miny - 2 * cell_h,
                    "top": maxy + 2 * cell_h,
                }
                tol = max(cell_w, cell_h) * 1.1
                assert b, "DEM bounds should be present"
                assert b.get("left") == pytest.approx(expected["left"], abs=tol)
                assert b.get("right") == pytest.approx(expected["right"], abs=tol)
                assert b.get("bottom") == pytest.approx(expected["bottom"], abs=tol)
                assert b.get("top") == pytest.approx(expected["top"], abs=tol)

                # Layers should share bounds/transform and cell sizes
                for layer in (soil, land_cover):
                    lb = layer.get("bounds") or {}
                    lt = layer.get("transform") or []
                    assert lb and lt and len(lt) >= 6, "Layer bounds/transform should be present"
                    assert lb.get("left") == pytest.approx(b.get("left"), abs=tol)
                    assert lb.get("right") == pytest.approx(b.get("right"), abs=tol)
                    assert lb.get("bottom") == pytest.approx(b.get("bottom"), abs=tol)
                    assert lb.get("top") == pytest.approx(b.get("top"), abs=tol)
                    lcw = abs(lt[0])
                    lch = abs(lt[4])
                    assert lcw == pytest.approx(cell_w, rel=1e-3, abs=1e-6)
                    assert lch == pytest.approx(cell_h, rel=1e-3, abs=1e-6)

                etl_layers = body.get("etl_layers") or {}
                assert etl_layers.get("dem", {}).get("status") == "ok"
                assert etl_layers.get("soil", {}).get("status") == "ok"
                assert etl_layers.get("land_cover", {}).get("status") == "ok"


@pytest.mark.integration
@pytest.mark.anyio
async def test_grid_endpoint_backfills_missing_layer_via_country_etl(platform_config, vhs):
    """
    If a requested layer is missing, the grid endpoint should trigger the country ETL for that layer.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user
    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123"})
    db.close()

    transport = ASGITransport(app=app)
    async with vhs("platform_grid_etl"):
        async with app.router.lifespan_context(app):
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                payload = {
                    "username": "alice",
                    "name": "Grid Backfill Project",
                    "geometry": _load_sample("valid_small.json"),
                }
                resp = await client.post("/api/platform/projects", json=payload)
                assert resp.status_code == 201
                project_id = resp.json().get("project_id")
                assert project_id

            # Simulate missing land cover
            analytics_client = AsyncIOMotorClient("mongodb://localhost:27017")
            await analytics_client.analytics.terrain.update_one(
                {"project_id": project_id}, {"$unset": {"land_cover": "", "etl_layers.land_cover": ""}}
            )
            terrain_missing = await analytics_client.analytics.terrain.find_one({"project_id": project_id})
            assert not terrain_missing.get("land_cover"), "Precondition: land_cover removed before backfill"
            analytics_client.close()

            async with AsyncClient(transport=transport, base_url="http://test") as client:
                resp = await client.get(f"/api/platform/projects/{project_id}/grid", params={"layer": "land_cover", "refresh": True})
                assert resp.status_code == 200
                body = resp.json()
                assert body.get("layer") == "land_cover"
                assert body.get("data"), "Land cover data should be present after backfill"
                etl_layers = body.get("etl_layers") or {}
                assert etl_layers.get("land_cover", {}).get("status") == "ok"

            # Ensure terrain now has land cover stored
            analytics_client = AsyncIOMotorClient("mongodb://localhost:27017")
            terrain = await analytics_client.analytics.terrain.find_one({"project_id": project_id})
            analytics_client.close()
            assert terrain and terrain.get("land_cover"), "Land cover should be persisted after backfill"
            assert terrain.get("etl_layers", {}).get("land_cover", {}).get("status") == "ok"

@pytest.mark.integration
@pytest.mark.anyio
async def test_project_deletion_respects_ownership(platform_config):
    """
    Project deletion should succeed for the owner and fail for non-owners or missing projects.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed users and projects
    db.connect()
    await db.get_db().users.insert_many(
        [{"username": "alice", "password": "pw123"}, {"username": "bob", "password": "pw456"}]
    )
    await db.get_db().projects.insert_many(
        [
            {"project_id": "p1", "username": "alice", "name": "A1"},
            {"project_id": "p2", "username": "bob", "name": "B1"},
        ]
    )
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Non-owner cannot delete
            resp = await client.request("DELETE", "/api/platform/projects/p2", json={"username": "alice"})
            assert resp.status_code in (403, 404), "Non-owner should not be able to delete another user's project"

            # Owner deletes successfully
            resp = await client.request("DELETE", "/api/platform/projects/p1", json={"username": "alice"})
            assert resp.status_code in (200, 204), "Owner should be able to delete their project"
            # Ensure terrain and grid are removed
            analytics_client = AsyncIOMotorClient("mongodb://localhost:27017")
            assert await analytics_client.analytics.terrain.find_one({"project_id": "p1"}) is None
            analytics_client.close()

            # Missing project returns 404
            resp = await client.request("DELETE", "/api/platform/projects/missing", json={"username": "alice"})
            assert resp.status_code == 404, "Deleting missing project should return 404"

@pytest.mark.integration
@pytest.mark.anyio
async def test_logout_invalidates_session(platform_config):
    """
    Logout should invalidate auth so subsequent protected calls fail.
    """
    cfg = platform_config
    db = PlatformDatabase(cfg)
    app = create_app(config=cfg, db=db)

    # Seed user and project
    db.connect()
    await db.get_db().users.insert_one({"username": "alice", "password": "pw123", "projects": ["p1"]})
    await db.get_db().projects.insert_one({"project_id": "p1", "username": "alice", "name": "A1"})
    db.close()

    transport = ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # Assume login issues a token (placeholder until implemented)
            resp = await client.post("/api/platform/login", json={"username": "alice", "password": "pw123"})
            token = resp.json().get("token") if resp.status_code == 200 else None

            # Call logout
            resp = await client.post("/api/platform/logout", headers={"Authorization": f"Bearer {token}"})
            assert resp.status_code in (200, 204), "Logout should succeed"

            # Subsequent protected call should fail
            resp = await client.get("/api/platform/projects")
            assert resp.status_code in (401, 403), "Protected route should reject after logout"

    # DB cleanup handled by fixture
