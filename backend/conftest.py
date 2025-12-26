"""
Shared pytest fixtures.

Provides a lightweight VHS-style cassette for recording/replaying HTTPX
requests during tests. This keeps integration tests deterministic while
still using real responses that can be refreshed on demand.
"""

import base64
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import os

import httpx
import pytest

CASSETTE_DIR = Path(__file__).resolve().parent / "tests" / "cassettes"


class Cassette:
    """
    Minimal HTTP recorder/replayer for httpx.AsyncClient.

    Modes:
      - "once"   (default): replay if a cassette exists; otherwise record and save.
      - "record": always hit the network and overwrite the cassette.
      - "replay": never hit the network; fail if no matching recording is available.
    """

    def __init__(self, path: Path, mode: str = "once", monkeypatch=None):
        self.path = path
        self.mode = mode
        self.monkeypatch = monkeypatch
        self._playback: List[Dict[str, Any]] = []
        self._records: List[Dict[str, Any]] = []
        self._cursor = 0
        self._orig_request = httpx.AsyncClient.request

    def _is_local(self, url: str) -> bool:
        if not url:
            return True
        target = str(url)
        return target.startswith(
            (
                "/",
                "http://test",
                "https://test",
                "http://localhost",
                "https://localhost",
                "http://127.0.0.1",
                "https://127.0.0.1",
            )
        )

    def _load(self):
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text())
                interactions = data.get("interactions") or data or []
                self._playback = [
                    rec
                    for rec in interactions
                    if not self._is_local((rec.get("request") or {}).get("url", ""))
                ]
            except Exception:
                self._playback = []

    def _save(self):
        if not self._records or self.mode == "replay":
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"interactions": self._records}
        self.path.write_text(json.dumps(payload, indent=2))

    def _pop_next(self, method: str, url: str) -> Optional[Dict[str, Any]]:
        target = str(url)
        for idx in range(self._cursor, len(self._playback)):
            rec = self._playback[idx]
            req = rec.get("request") or {}
            if req.get("method") == method and req.get("url") == target:
                self._cursor = idx + 1
                return rec
        return None

    def _serialize_response(self, method: str, url: str, kwargs: dict, resp: httpx.Response) -> Dict[str, Any]:
        body_b64 = base64.b64encode(resp.content or b"").decode("ascii")
        req_body = kwargs.get("content") or kwargs.get("data") or kwargs.get("json")
        return {
            "request": {"method": method, "url": url, "body": req_body},
            "response": {
                "status": resp.status_code,
                "headers": dict(resp.headers),
                "body_b64": body_b64,
            },
        }

    def _response_from_record(self, record: Dict[str, Any]) -> httpx.Response:
        resp_meta = record.get("response", {})
        content = base64.b64decode(resp_meta.get("body_b64", "") or b"")
        request_meta = record.get("request", {})
        req = httpx.Request(request_meta.get("method", "GET"), request_meta.get("url", ""))
        return httpx.Response(
            status_code=resp_meta.get("status", 200),
            headers=resp_meta.get("headers") or {},
            content=content,
            request=req,
        )

    async def _patched_request(self, client, method: str, url: str, *args, **kwargs):
        target_url = str(url)
        if self._is_local(target_url):
            return await self._orig_request(client, method, url, *args, **kwargs)

        record = self._pop_next(method, target_url)
        if record and self.mode in {"once", "replay"}:
            return self._response_from_record(record)
        if self.mode == "replay":
            raise RuntimeError(f"No VHS recording for {method} {target_url}")
        resp = await self._orig_request(client, method, url, *args, **kwargs)
        self._records.append(self._serialize_response(method, target_url, kwargs, resp))
        return resp

    async def __aenter__(self):
        self._load()
        if self.monkeypatch:
            cassette = self

            async def _wrapper(client, method: str, url: str, *args, **kwargs):
                return await cassette._patched_request(client, method, url, *args, **kwargs)

            self.monkeypatch.setattr(httpx.AsyncClient, "request", _wrapper)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.monkeypatch:
            self.monkeypatch.setattr(httpx.AsyncClient, "request", self._orig_request)
        self._save()


@pytest.fixture
def vhs(monkeypatch):
    """
    Use as: async with vhs("dem_etl"): ...
    """

    def factory(name: str, mode: str = "once") -> Cassette:
        cassette_path = CASSETTE_DIR / f"{name}.json"
        clear = os.getenv("VHS_CLEAR") or os.getenv("PYTEST_VHS_CLEAR")
        if clear and cassette_path.exists():
            cassette_path.unlink(missing_ok=True)
        effective_mode = os.getenv("VHS_MODE") or mode
        return Cassette(cassette_path, mode=effective_mode, monkeypatch=monkeypatch)

    return factory
