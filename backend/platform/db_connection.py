"""
MongoDB connection wrapper for the platform database.

Keeps connection lifecycle centralized so callers do not instantiate clients
directly throughout the codebase.
"""

from motor.motor_asyncio import AsyncIOMotorClient

from backend.platform.config import PlatformConfig


class PlatformDatabase:
    def __init__(self, config: PlatformConfig):
        self.config = config
        self.client: AsyncIOMotorClient | None = None

    def connect(self) -> None:
        """Initialize the Motor client for the configured Mongo URL."""
        self.client = AsyncIOMotorClient(
            self.config.mongo_url,
            uuidRepresentation="standard",
            serverSelectionTimeoutMS=2000,
        )

    def get_db(self):
        """Return the configured logical database."""
        if not self.client:
            raise RuntimeError("Database client is not connected")
        return self.client[self.config.mongo_db]

    def close(self) -> None:
        """Close the client if it was created; safe to call multiple times."""
        if self.client:
            self.client.close()
            self.client = None


# Convenience instance for runtime use.
platform_db = PlatformDatabase(PlatformConfig.from_env())
