"""Platform unit tests: config, DB wrapper, and startup helpers."""

import pytest

from backend import ensure_platform_collections, REQUIRED_COLLECTIONS
from backend.platform import db_connection
from backend.platform.config import PlatformConfig


# --- PlatformConfig ---

def _clear_env(monkeypatch):
    keys = [
        "PLATFORM_MONGO_URL",
        "PLATFORM_DB_NAME",
        "PLATFORM_AUTH_SECRET",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def test_platform_config_defaults(monkeypatch):
    """
    The config object should populate sensible defaults when no env vars are set.
    """
    _clear_env(monkeypatch)

    cfg = PlatformConfig.from_env()

    assert cfg.mongo_url == "mongodb://localhost:27017", "default mongo URL should target local mongo"
    assert cfg.mongo_db == "platform", "default platform DB name should be 'platform'"
    assert cfg.auth_secret == "dev-secret", "default auth secret should be the dev placeholder"


# --- PlatformDatabase ---


class FakeMotorClient:
    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs
        self.closed = False

    def __getitem__(self, name):
        return {"db_name": name}

    def close(self):
        self.closed = True


@pytest.fixture
def config():
    return PlatformConfig(
        mongo_url="mongodb://example:27017",
        mongo_db="platform_test",
        auth_secret="",
    )


def test_connect_and_get_db(monkeypatch, config):
    """
    connect() should build a Motor client with the configured URL and return the named DB.
    """
    created = {}

    def fake_client(url, **kwargs):
        created["client"] = FakeMotorClient(url, **kwargs)
        return created["client"]

    monkeypatch.setattr(db_connection, "AsyncIOMotorClient", fake_client)

    db = db_connection.PlatformDatabase(config)
    db.connect()

    assert isinstance(created.get("client"), FakeMotorClient), "connect() must instantiate Motor client"
    assert created["client"].url == "mongodb://example:27017", "Motor client should receive configured mongo URL"
    assert created["client"].kwargs.get("uuidRepresentation") == "standard", "Client must set uuidRepresentation=standard"
    assert db.get_db() == {"db_name": "platform_test"}, "get_db() should return the configured DB handle"


def test_close_is_idempotent(config):
    """
    close() should be safe to call even when connect() was never called.
    """
    db = db_connection.PlatformDatabase(config)
    db.close()  # should not raise
    db.close()  # should still not raise


def test_close_closes_client(monkeypatch, config):
    """
    close() should call the client's close() when connected.
    """
    created = {}

    def fake_client(url, **kwargs):
        created["client"] = FakeMotorClient(url, **kwargs)
        return created["client"]

    monkeypatch.setattr(db_connection, "AsyncIOMotorClient", fake_client)

    db = db_connection.PlatformDatabase(config)
    db.connect()
    db.close()

    assert created["client"].closed is True, "close() must close the underlying Motor client"


# --- Startup helpers ---


class FakeDB:
    def __init__(self, existing=None):
        self.collections = set(existing or [])
        self.created = []

    async def list_collection_names(self):
        return list(self.collections)

    async def create_collection(self, name):
        self.collections.add(name)
        self.created.append(name)


@pytest.mark.anyio
async def test_ensure_platform_collections_creates_missing():
    """
    When collections are missing, they should be created.
    """
    db = FakeDB(existing=[])

    await ensure_platform_collections(db)

    assert set(db.collections) == set(REQUIRED_COLLECTIONS), "all required collections should exist"
    assert set(db.created) == set(REQUIRED_COLLECTIONS), "missing collections should be created"


@pytest.mark.anyio
async def test_ensure_platform_collections_skips_existing():
    """
    Existing collections should not be recreated.
    """
    db = FakeDB(existing=["users"])

    await ensure_platform_collections(db)

    assert set(db.collections) == {"users", "projects", "sessions"}, "missing collections should be added, existing preserved"
    assert set(db.created) == {"projects", "sessions"}, "only missing collections should be created"


# --- Signup validation ---


def test_signup_validator_rejects_missing_fields():
    from backend.platform import utils as validators

    with pytest.raises(ValueError):
        validators.validate_signup({})

    with pytest.raises(ValueError):
        validators.validate_signup({"username": "alice"})

    with pytest.raises(ValueError):
        validators.validate_signup({"password": "pw"})


def test_signup_validator_rejects_short_username_or_password():
    from backend.platform import utils as validators

    with pytest.raises(ValueError):
        validators.validate_signup({"username": "al", "password": "pw123"})

    with pytest.raises(ValueError):
        validators.validate_signup({"username": "alice", "password": "pw"})


def test_signup_validator_returns_clean_payload():
    from backend.platform import utils as validators

    payload = validators.validate_signup({"username": " Alice ", "password": " pw123 "})
    assert payload == {"username": "alice", "password": "pw123"}


# --- Login validation ---


def test_login_validator_requires_username_and_password():
    from backend.platform import utils as validators

    with pytest.raises(ValueError):
        validators.validate_login({})
    with pytest.raises(ValueError):
        validators.validate_login({"username": "alice"})
    with pytest.raises(ValueError):
        validators.validate_login({"password": "pw"})


def test_login_validator_trims_and_normalizes():
    from backend.platform import utils as validators

    payload = validators.validate_login({"username": " Alice ", "password": " pw123 "})
    assert payload == {"username": "alice", "password": "pw123"}


# --- Project creation validation ---


def test_project_validator_requires_username_and_geometry():
    from backend.platform import utils as validators

    with pytest.raises(ValueError):
        validators.validate_project_create({})
    with pytest.raises(ValueError):
        validators.validate_project_create({"username": "alice"})
    with pytest.raises(ValueError):
        validators.validate_project_create({"geometry": {"type": "Polygon", "coordinates": []}})


def test_project_validator_accepts_optional_name_and_returns_clean_payload():
    from backend.platform import utils as validators

    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    payload = validators.validate_project_create(
        {"username": " Alice ", "name": " Test ", "geometry": geometry}
    )
    assert payload["username"] == "alice"
    assert payload["name"] == "Test"
    assert payload["geometry"] == geometry


# --- Project naming ---


def test_project_name_uses_provided_name(monkeypatch):
    from datetime import datetime
    from backend.platform import utils as naming

    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
    calls = []

    def fake_resolve(geom):
        calls.append(geom)
        return {"country": "USA", "subdivision": "CA"}

    monkeypatch.setattr(naming, "analytics_services", type("S", (), {"resolve_region": fake_resolve}))

    name = naming.generate_project_name(geometry=geometry, provided_name=" My Project ", now=datetime(2023, 1, 2, 3, 4, 5))
    assert name == "My Project"
    assert calls == [geometry], "Should call analytics to resolve region even when name is provided"


def test_project_name_falls_back_to_region_and_timestamp(monkeypatch):
    from datetime import datetime
    from backend.platform import utils as validators

    geometry = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}

    def fake_resolve(_geom):
        return {"country": "USA", "subdivision": "CA"}

    monkeypatch.setattr(validators, "analytics_services", type("S", (), {"resolve_region": fake_resolve}))

    fixed_now = datetime(2023, 1, 2, 3, 4, 5)
    name = validators.generate_project_name(geometry=geometry, provided_name=None, now=fixed_now)
    assert name == "USA-CA-20230102T030405Z"


# --- Area bounds ---


def test_area_bounds_rejects_out_of_range(monkeypatch):
    from backend.platform import utils as validators

    def fake_area(_geom):
        return 50  # hectares

    monkeypatch.setattr(validators, "analytics_services", type("S", (), {"compute_area_hectares": fake_area}))

    with pytest.raises(ValueError):
        validators.validate_area_bounds({"geometry": {}})


def test_area_bounds_accepts_valid_area(monkeypatch):
    from backend.platform import utils as validators

    def fake_area(_geom):
        return 150  # hectares

    monkeypatch.setattr(validators, "analytics_services", type("S", (), {"compute_area_hectares": fake_area}))

    area = validators.validate_area_bounds({"geometry": {}})
    assert area == 150


# --- Ownership checks ---


def test_ownership_check_allows_owner(monkeypatch):
    from backend.platform import utils as validators

    project = {"project_id": "p1", "username": "alice"}
    validators.require_project_ownership(project, "alice")  # should not raise


def test_ownership_check_rejects_non_owner(monkeypatch):
    from backend.platform import utils as validators

    project = {"project_id": "p1", "username": "alice"}
    with pytest.raises(PermissionError):
        validators.require_project_ownership(project, "bob")
