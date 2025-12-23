"""
Platform-level configuration loader.

Reads environment variables into a simple value object so the rest of the
application can consume strongly named settings instead of hitting os.getenv
throughout the codebase.
"""

import os
from dataclasses import dataclass


@dataclass
class PlatformConfig:
    mongo_url: str
    mongo_db: str
    auth_secret: str

    @classmethod
    def from_env(cls) -> "PlatformConfig":
        mongo_url = os.getenv("PLATFORM_MONGO_URL", "mongodb://localhost:27017")
        mongo_db = os.getenv("PLATFORM_DB_NAME", "platform")
        auth_secret = os.getenv("PLATFORM_AUTH_SECRET", "dev-secret")

        return cls(
            mongo_url=mongo_url,
            mongo_db=mongo_db,
            auth_secret=auth_secret,
        )
