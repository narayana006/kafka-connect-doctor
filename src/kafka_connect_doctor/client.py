"""Minimal Kafka Connect REST client. Injectable so it can be replaced by a fake in tests.

Only two calls are made:
- GET  /connectors?expand=status&expand=info       (read-only; Connect 2.3+)
- POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true
  (Connect 3.0+, KIP-745; only when the user passes --apply)
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

STATUS_PATH = "/connectors?expand=status&expand=info"


def restart_path(name: str) -> str:
    return f"/connectors/{urllib.parse.quote(name, safe='')}/restart?includeTasks=true&onlyFailed=true"


class ConnectClient(Protocol):
    def get_json(self, path: str) -> Any: ...

    def post(self, path: str) -> tuple[int, Any]: ...


class ClientError(RuntimeError):
    pass


class UrllibConnectClient:
    """Talks to one Connect worker over HTTP(S) with the standard library."""

    def __init__(self, base_url: str, timeout: float = 10.0, opener: Any = None) -> None:
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ClientError(f"invalid Connect URL {base_url!r}; expected http(s)://host:port")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._open = opener or urllib.request.urlopen

    def _request(self, method: str, path: str) -> tuple[int, Any]:
        req = urllib.request.Request(
            self.base_url + path,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            data=b"" if method == "POST" else None,
        )
        try:
            with self._open(req, timeout=self.timeout) as resp:
                body = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            body = exc.read()
            status = exc.code
        except (urllib.error.URLError, OSError) as exc:
            raise ClientError(f"{method} {self.base_url}{path} failed: {exc}") from exc
        text = body.decode("utf-8", "replace") if body else ""
        try:
            parsed = json.loads(text) if text else None
        except json.JSONDecodeError:
            parsed = text
        return status, parsed

    def get_json(self, path: str) -> Any:
        status, body = self._request("GET", path)
        if status != 200:
            raise ClientError(f"GET {path} returned HTTP {status}: {body}")
        return body

    def post(self, path: str) -> tuple[int, Any]:
        return self._request("POST", path)
