"""
Platform app factory and initialization helpers.

Creates the FastAPI application, wires health check, runs engine initializers on
startup, ensures core collections exist, and manages the platform DB lifecycle.
"""

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Iterable, Awaitable, Callable, Optional
from datetime import datetime, timezone

# Avoid shadowing the stdlib 'platform' module when tests run from the backend dir.
if "" in __import__("sys").path:
    __import__("sys").path.remove("")
    __import__("sys").path.append("")

from fastapi import FastAPI, APIRouter, HTTPException, status, Depends, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from backend.platform.config import PlatformConfig
from backend.platform.db_connection import PlatformDatabase, platform_db
from backend.platform import utils
from backend.services import analytics, operations, optimizations

Initializer = Callable[[], Awaitable[None]]

REQUIRED_COLLECTIONS = ["users", "projects", "sessions"]
logger = logging.getLogger("landos.platform")


async def ensure_platform_collections(db) -> None:
    """
    Ensure core collections exist on the platform database.
    """
    existing = set(await db.list_collection_names())
    for name in REQUIRED_COLLECTIONS:
        if name not in existing:
            await db.create_collection(name)
    logger.info("Platform collections ready: %s", REQUIRED_COLLECTIONS)


def _create_lifespan(
    db: PlatformDatabase,
    initializers: Iterable[Initializer],
):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        db.connect()
        logger.info("Connected platform DB")
        await ensure_platform_collections(db.get_db())
        logger.info("Ensured platform collections: %s", REQUIRED_COLLECTIONS)
        for init in initializers:
            logger.info("Running initializer %s", getattr(init, "__name__", str(init)))
            await init()
        app.state.engines_ready = True
        yield
        # Shutdown
        db.close()
        logger.info("Closed platform DB")

    return lifespan


def _hash(password: str, cfg: PlatformConfig) -> str:
    return utils.hash_password(password, cfg.auth_secret)


def create_app(
    config: Optional[PlatformConfig] = None,
    db: Optional[PlatformDatabase] = None,
    engine_initializers: Optional[Iterable[Initializer]] = None,
    engine_routers: Optional[Iterable[APIRouter]] = None,
) -> FastAPI:
    """
    Build the FastAPI app with provided configuration, DB, initializers, and routers.
    """
    cfg = config or PlatformConfig.from_env()
    database = db or platform_db
    default_initializers = [
        analytics.initialize,
        operations.initialize,
        optimizations.initialize,
    ]
    default_routers = [
        analytics.router,
        operations.router,
        optimizations.router,
    ]
    initializers = list(engine_initializers) if engine_initializers is not None else default_initializers
    routers = list(engine_routers) if engine_routers is not None else default_routers

    app = FastAPI(
        title="LandOS Platform API",
        version="0.1.0",
        lifespan=_create_lifespan(database, initializers),
    )
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # CORS: allow local dev frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.config = cfg
    app.state.db = database

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    platform_router = APIRouter(prefix="/api/platform", tags=["platform"])

    def _db():
        return database.get_db()

    def _clean_username(value: str) -> str:
        return value.strip().lower()

    async def _require_token(authorization: str = Header(None)):
        if not authorization or not authorization.lower().startswith("bearer "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
        token = authorization.split(" ", 1)[1]
        session = await _db().sessions.find_one({"token": token})
        if not session:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        return session

    @platform_router.post("/signup", status_code=status.HTTP_201_CREATED)
    async def signup(payload: dict):
        try:
            cleaned = utils.validate_signup(payload)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        users = _db().users
        existing = await users.find_one({"username": cleaned["username"]})
        if existing:
            logger.warning("Signup rejected for existing user '%s'", cleaned["username"])
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User already exists")
        hashed = _hash(cleaned["password"], cfg)
        await users.insert_one({"username": cleaned["username"], "password": hashed, "projects": []})
        logger.info("User '%s' created", cleaned["username"])
        return {"ok": True}

    @platform_router.options("/signup")
    async def signup_options():
        return JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content=None)

    @platform_router.post("/login")
    async def login(payload: dict):
        try:
            cleaned = utils.validate_login(payload)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
        user = await _db().users.find_one({"username": cleaned["username"]})
        if not user:
            logger.warning("Login failed for missing user '%s'", cleaned["username"])
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"ok": False, "error": "Invalid credentials"})
        stored_pw = user.get("password")
        candidate_hash = _hash(cleaned["password"], cfg)
        if stored_pw not in (candidate_hash, cleaned["password"]):
            logger.warning("Login failed for invalid password user '%s'", cleaned["username"])
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"ok": False, "error": "Invalid credentials"})
        token = secrets.token_hex(16)
        await _db().sessions.insert_one({"username": cleaned["username"], "token": token})
        logger.info("Login success for user '%s'", cleaned["username"])
        return {"ok": True, "token": token}

    @platform_router.options("/login")
    async def login_options():
        return JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content=None)

    @platform_router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
    async def logout(session=Depends(_require_token)):
        await _db().sessions.delete_one({"token": session["token"]})
        logger.info("Logout for user '%s'", session["username"])
        return JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content=None)

    @platform_router.get("/projects")
    async def list_projects_get(session=Depends(_require_token)):
        username = session["username"]
        projects = await _db().projects.find({"username": username}, {"_id": 0}).to_list(None)
        logger.info("Listed %d projects for user '%s'", len(projects), username)
        return projects

    async def _trigger_soil_etl(project_id: str, geometry: dict | None):
        try:
            loop = asyncio.get_running_loop()
            async def _run():
                try:
                    await analytics.api.soil.fetch_soil_data({"project_id": project_id, "geometry": geometry})
                except Exception as exc:
                    logger.exception("Async soil ETL retry failed for project %s: %s", project_id, exc)
            loop.create_task(_run())
        except Exception as exc:
            logger.exception("Failed to schedule soil ETL for project %s: %s", project_id, exc)

    @platform_router.get("/projects/{project_id}/grid")
    async def get_grid(project_id: str, layer: str | None = None, refresh: bool | None = False):
        """
        Return grid layers for a project. If layer is provided, filter to that layer.
        Currently supports 'dem' via analytics terrain collection.
        """
        # Fetch terrain from analytics DB
        terrain = await analytics.db.get_db().terrain.find_one({"project_id": project_id})
        if not terrain:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Grid not found")
        logger.info("Grid request project=%s layer=%s", project_id, layer or "all")
        dem = terrain.get("elevation_data")
        soil = terrain.get("soil_data")
        etl_layers = terrain.get("etl_layers") or {}
        if refresh and (layer in (None, "soil")):
            if not soil or (etl_layers.get("soil", {}).get("status") == "failed"):
                project = await _db().projects.find_one({"project_id": project_id}) or {}
                await _trigger_soil_etl(project_id, project.get("geometry"))
        layers = {}
        if dem:
            layers["dem"] = dem
        if soil:
            layers["soil"] = soil
        if layer:
            filtered = layers.get(layer)
            if not filtered:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Requested layer not found")
            return {"project_id": project_id, "layer": layer, "data": filtered, "etl_layers": etl_layers}
        return {"project_id": project_id, "layers": layers, "etl_layers": etl_layers}

    @platform_router.post("/projects")
    async def create_or_list_projects(payload: dict):
        # If geometry missing, treat as list-by-username
        if "geometry" not in payload:
            username = payload.get("username")
            if not username:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username required")
            projects = await _db().projects.find({"username": username}, {"_id": 0}).to_list(None)
            logger.info("Listed %d projects for user '%s' (POST)", len(projects), username)
            return projects

        try:
            cleaned = utils.validate_project_create(payload)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        # Validate geometry area bounds
        try:
            geometry = analytics.validate_geometry(cleaned["geometry"])
            area = await analytics.compute_area_hectares(geometry)
        except Exception as exc:
            logger.error("Geometry validation failed for user '%s': %s", cleaned.get("username"), exc)
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"error": str(exc)})
        if area < 100 or area > 1000:
            logger.warning("Area out of range (%.2f ha) for user '%s'", area, cleaned.get("username"))
            return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"error": "geometry area out of range"})

        # Resolve region and name
        try:
            region = await analytics.resolve_region(geometry)
        except Exception:
            logger.exception("Region resolution failed for user '%s'", cleaned.get("username"))
            region = {"country": None, "country_name": None, "subdivision": None, "subdivision_name": None}
        if cleaned.get("name"):
            name = cleaned["name"]
        else:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            name = f"{region.get('country_name','UNK')}-{region.get('subdivision_name','UNK')}-{ts}"

        project_id = secrets.token_hex(8)
        project_doc = {
            "project_id": project_id,
            "username": cleaned["username"],
            "name": name,
            "geometry": cleaned["geometry"],
            "country": region.get("country"),
            "country_name": region.get("country_name"),
            "subdivision": region.get("subdivision"),
            "subdivision_name": region.get("subdivision_name"),
            "area_hectares": area,
            "created": datetime.now(timezone.utc).isoformat(),
        }
        await _db().projects.insert_one(project_doc)
        await _db().users.update_one(
            {"username": cleaned["username"]},
            {"$addToSet": {"projects": project_id}},
        )
        await _db().projects.update_one({"project_id": project_id}, {"$set": {"status": "etl_pending"}})
        # Trigger ETL for DEM and other layers
        try:
            loop = asyncio.get_running_loop()
            async def _run():
                try:
                    await analytics.trigger_etl(project_doc)
                    await _db().projects.update_one({"project_id": project_id}, {"$set": {"status": "ready"}})
                except Exception as exc:
                    logger.exception("ETL trigger failed for project %s: %s", project_id, exc)
                    await _db().projects.update_one({"project_id": project_id}, {"$set": {"status": "etl_failed", "etl_error": str(exc)}})
            loop.create_task(_run())
        except Exception as exc:
            logger.exception("ETL trigger failed for project %s: %s", project_id, exc)
        logger.info("Project '%s' created for user '%s' country=%s subdivision=%s area=%.2f",
                    project_id, cleaned["username"], region.get("country"), region.get("subdivision"), area)
        return JSONResponse(status_code=status.HTTP_201_CREATED, content={"ok": True, "project_id": project_id})

    @platform_router.delete("/projects/{project_id}")
    async def delete_project(project_id: str, payload: dict | None = None):
        username = (payload or {}).get("username") if isinstance(payload, dict) else None
        if not username:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="username required")
        project = await _db().projects.find_one({"project_id": project_id})
        if not project:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="project not found")
        try:
            utils.require_project_ownership(project, _clean_username(username))
        except PermissionError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
        await _db().projects.delete_one({"project_id": project_id})
        await _db().users.update_one(
            {"username": project["username"]},
            {"$pull": {"projects": project_id}},
        )
        # Clean up terrain/grid in analytics
        try:
            await analytics.db.get_db().terrain.delete_one({"project_id": project_id})
            logger.info("Deleted terrain for project %s", project_id)
        except Exception:
            logger.exception("Failed to delete terrain for project %s", project_id)
        from fastapi import Response
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    for router in routers + [platform_router]:
        app.include_router(router)

    return app
