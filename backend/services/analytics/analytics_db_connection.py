"""
Connection handling for the analytics MongoDB database.
"""

from motor.motor_asyncio import AsyncIOMotorClient
import logging

from backend.services.analytics import config

logger = logging.getLogger("landos.analytics")


class AnalyticsDatabase:
    def __init__(self, mongo_url: str | None = None, db_name: str | None = None):
        self.mongo_url = mongo_url or config.MONGO_URL
        self.db_name = db_name or config.MONGO_DB
        self.client: AsyncIOMotorClient | None = None

    def connect(self):
        self.client = AsyncIOMotorClient(
            self.mongo_url,
            uuidRepresentation="standard",
            serverSelectionTimeoutMS=2000,
        )
        logger.info("Connected analytics Mongo @ %s (db=%s)", self.mongo_url, self.db_name)
        return self

    def get_db(self):
        if not self.client:
            raise RuntimeError("Analytics DB not connected")
        return self.client[self.db_name]

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Closed analytics Mongo connection")


# Shared instance
analytics_db = AnalyticsDatabase()
