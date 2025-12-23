"""
Ping service for analytics readiness.
"""


async def ping():
    return {"engine": "analytics", "ok": True}
