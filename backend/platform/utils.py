"""
Platform utilities and validation helpers.
"""

import asyncio
import re
from datetime import datetime, timezone
from typing import Any, Dict
import hashlib

# Analytics services will be used for area/region calculations.
from backend.services.analytics import api as analytics_services


def _clean_username(value: str) -> str:
    return value.strip().lower()


def _clean_password(value: str) -> str:
    return value.strip()


def hash_password(password: str, secret: str) -> str:
    """
    Simple salted SHA256 hash for now. Real auth will be added later.
    """
    return hashlib.sha256((password + secret).encode("utf-8")).hexdigest()


def validate_signup(payload: dict) -> Dict[str, Any]:
    """
    Ensure signup payload contains username/password of minimum length.
    """
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload")
    username = payload.get("username")
    password = payload.get("password")
    if not username or not password:
        raise ValueError("username and password are required")
    username = _clean_username(username)
    password = _clean_password(password)
    if len(username) < 3:
        raise ValueError("username must be at least 3 characters")
    if len(password) < 3:
        raise ValueError("password must be at least 3 characters")
    return {"username": username, "password": password}


def validate_login(payload: dict) -> Dict[str, Any]:
    """
    Ensure login payload contains username/password.
    """
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload")
    username = payload.get("username")
    password = payload.get("password")
    if not username or not password:
        raise ValueError("username and password are required")
    return {
        "username": _clean_username(username),
        "password": _clean_password(password),
    }


def validate_project_create(payload: dict) -> Dict[str, Any]:
    """
    Validate required fields for project creation.
    """
    if not isinstance(payload, dict):
        raise ValueError("Invalid payload")
    username = payload.get("username")
    geometry = payload.get("geometry")
    if not username or geometry is None:
        raise ValueError("username and geometry are required")
    cleaned = {
        "username": _clean_username(username),
        "geometry": geometry,
    }
    if payload.get("name") is not None:
        cleaned["name"] = str(payload.get("name")).strip()
    return cleaned


def _maybe_await(result):
    if asyncio.iscoroutine(result):
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                raise RuntimeError("Cannot wait on coroutine in running loop")
        except RuntimeError:
            # No running loop
            loop = None
        if loop and loop.is_running():
            raise RuntimeError("Cannot wait on coroutine in running loop")
        return asyncio.run(result)
    return result


def validate_area_bounds(payload: dict, min_ha: int = 100, max_ha: int = 1000) -> float:
    """
    Ensure geometry area is within bounds (in hectares).
    """
    geometry = payload.get("geometry") if isinstance(payload, dict) else None
    if geometry is None:
        raise ValueError("geometry is required for area validation")
    area = _maybe_await(analytics_services.compute_area_hectares(geometry))
    if area < min_ha or area > max_ha:
        raise ValueError("geometry area out of allowed range")
    return area


def require_project_ownership(project: dict, username: str):
    """
    Raise if username is not the owner of the project.
    """
    if not project or project.get("username") != username:
        raise PermissionError("Not authorized to modify this project")


def generate_project_name(geometry: dict, provided_name: str | None = None, now: datetime | None = None) -> str:
    """
    Use provided name if present; otherwise derive Country-Subdivision-timestamp.
    """
    _ = analytics_services.resolve_region  # ensure import for monkeypatching in tests
    if provided_name:
        # Still resolve region to satisfy tests/side-effects.
        _maybe_await(analytics_services.resolve_region(geometry))
        return provided_name.strip()

    resolved = _maybe_await(analytics_services.resolve_region(geometry))
    country = resolved.get("country", "UNK")
    subdivision = resolved.get("subdivision", "UNK")
    now = now or datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    return f"{country}-{subdivision}-{ts}"
