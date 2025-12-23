"""
Scheduler helpers placeholder.
"""

from datetime import datetime, timedelta

# Allow monkeypatching in tests
utcnow = datetime.utcnow


def build_dataset_metadata(name: str, downloaded_at: datetime) -> dict:
    return {
        "name": name,
        "downloaded_at": downloaded_at,
        "expires_at": downloaded_at + timedelta(days=365),
    }


def build_refresh_job(name: str, interval_days: int) -> dict:
    return {
        "name": name,
        "next_run_at": utcnow() + timedelta(days=interval_days),
        "interval_days": interval_days,
    }
