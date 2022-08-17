"""Load Kafka Connect REST API status JSON into a small, uniform model.

Accepted shapes (all as returned by the Connect REST API):
- GET /connectors?expand=status[&expand=info]  -> {"name": {"status": {...}, "info": {...}}}
- GET /connectors/{name}/status                -> {"name": ..., "connector": {...}, "tasks": [...]}
- a JSON list of /status objects (handy when concatenating saved files)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class InputError(ValueError):
    """Raised when the JSON does not look like Connect REST API output."""


@dataclass
class TaskStatus:
    id: int
    state: str
    worker_id: str | None
    trace: str | None = None


@dataclass
class ConnectorStatus:
    name: str
    state: str
    worker_id: str | None
    type: str | None
    trace: str | None = None
    tasks: list[TaskStatus] = field(default_factory=list)
    config: dict[str, str] | None = None

    @property
    def failed_tasks(self) -> list[TaskStatus]:
        return [t for t in self.tasks if t.state == "FAILED"]

    @property
    def connector_failed(self) -> bool:
        return self.state == "FAILED"

    @property
    def connector_class(self) -> str | None:
        if self.config:
            return self.config.get("connector.class")
        return None


def _is_status_object(obj: Any) -> bool:
    return isinstance(obj, dict) and "connector" in obj and "tasks" in obj and isinstance(obj.get("connector"), dict)


def _parse_status(obj: dict[str, Any], config: dict[str, str] | None = None, fallback_name: str | None = None) -> ConnectorStatus:
    name = obj.get("name") or fallback_name
    if not name:
        raise InputError("connector status object has no name")
    conn = obj["connector"]
    if "state" not in conn:
        raise InputError(f"connector {name!r}: status has no connector.state")
    tasks = []
    for t in obj.get("tasks") or []:
        if "id" not in t or "state" not in t:
            raise InputError(f"connector {name!r}: task entry without id/state: {t!r}")
        tasks.append(TaskStatus(id=int(t["id"]), state=t["state"], worker_id=t.get("worker_id"), trace=t.get("trace")))
    tasks.sort(key=lambda t: t.id)
    return ConnectorStatus(
        name=name,
        state=conn["state"],
        worker_id=conn.get("worker_id"),
        type=obj.get("type"),
        trace=conn.get("trace"),
        tasks=tasks,
        config=config,
    )


def load_connectors(doc: Any) -> list[ConnectorStatus]:
    """Normalise any accepted shape into a list of ConnectorStatus, sorted by name."""
    out: list[ConnectorStatus] = []
    if isinstance(doc, list):
        for item in doc:
            out.extend(load_connectors(item))
    elif _is_status_object(doc):
        out.append(_parse_status(doc))
    elif isinstance(doc, dict):
        if not doc:
            return []
        for name, entry in doc.items():
            if not isinstance(entry, dict) or "status" not in entry:
                if isinstance(entry, dict) and "info" in entry:
                    raise InputError(
                        f"connector {name!r} has only 'info'; fetch with ?expand=status (optionally also &expand=info)"
                    )
                raise InputError(
                    "unrecognised JSON: expected /connectors?expand=status output, a /connectors/{name}/status "
                    "object, or a list of those"
                )
            info = entry.get("info") or {}
            out.append(_parse_status(entry["status"], config=info.get("config"), fallback_name=name))
    else:
        raise InputError("unrecognised JSON: expected an object or a list")
    return out


def merge(connectors: list[ConnectorStatus]) -> list[ConnectorStatus]:
    """De-duplicate by name (last one wins, but keep a config seen earlier) and sort."""
    by_name: dict[str, ConnectorStatus] = {}
    for c in connectors:
        prev = by_name.get(c.name)
        if prev is not None and c.config is None:
            c.config = prev.config
        by_name[c.name] = c
    return [by_name[n] for n in sorted(by_name)]
