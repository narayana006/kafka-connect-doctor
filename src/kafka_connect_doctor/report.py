"""Turn connector statuses into findings, a per-worker summary and a restart plan."""

from __future__ import annotations

import shlex
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import quote

from .classify import Diagnosis, classify
from .client import restart_path
from .model import ConnectorStatus


@dataclass
class Failure:
    connector: str
    task_id: int | None  # None = the connector instance itself
    worker_id: str | None
    diagnosis: Diagnosis

    @property
    def label(self) -> str:
        return f"{self.connector} task {self.task_id}" if self.task_id is not None else f"{self.connector} (connector)"


@dataclass
class ConnectorFinding:
    connector: ConnectorStatus
    failures: list[Failure] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return not self.failures and not self.warnings

    @property
    def action(self) -> str:
        """'restart', 'fix-first', or 'none'."""
        if not self.failures:
            return "none"
        if all(f.diagnosis.category.restart_helps for f in self.failures):
            return "restart"
        return "fix-first"


@dataclass
class WorkerSummary:
    worker_id: str
    connectors: int = 0
    tasks: int = 0
    failed_tasks: int = 0
    failed_connectors: int = 0
    categories: Counter[str] = field(default_factory=Counter)


@dataclass
class Report:
    findings: list[ConnectorFinding]
    workers: list[WorkerSummary]
    worker_notes: list[str]

    @property
    def failures(self) -> list[Failure]:
        return [f for c in self.findings for f in c.failures]

    @property
    def counts(self) -> dict[str, int]:
        return {
            "connectors": len(self.findings),
            "tasks": sum(len(c.connector.tasks) for c in self.findings),
            "failed_connectors": sum(1 for c in self.findings if c.connector.connector_failed),
            "failed_tasks": sum(len(c.connector.failed_tasks) for c in self.findings),
            "connectors_with_warnings": sum(1 for c in self.findings if c.warnings),
        }


def _warnings_for(c: ConnectorStatus) -> list[str]:
    out: list[str] = []
    if c.state == "RUNNING" and not c.tasks:
        out.append(
            "connector is RUNNING but has no tasks: it may still be starting, or it could not generate "
            "task configs (for example it cannot reach its source); check the worker log"
        )
    if c.state == "UNASSIGNED":
        out.append("connector is UNASSIGNED: no worker has picked it up (rebalance in progress or a worker left)")
    unassigned = [t.id for t in c.tasks if t.state == "UNASSIGNED"]
    if unassigned:
        out.append(f"task(s) {', '.join(map(str, unassigned))} UNASSIGNED: waiting for a worker (rebalance or lost worker)")
    if c.state in ("PAUSED", "STOPPED"):
        out.append(f"connector is {c.state} on purpose or by an operator; it does not process data until resumed")
    restarting = [t.id for t in c.tasks if t.state == "RESTARTING"]
    if c.state == "RESTARTING" or restarting:
        out.append("a restart is in progress; re-check the status in a few seconds")
    return out


def diagnose(connectors: list[ConnectorStatus]) -> Report:
    findings: list[ConnectorFinding] = []
    workers: dict[str, WorkerSummary] = {}

    def worker(wid: str | None) -> WorkerSummary:
        key = wid or "(unassigned)"
        return workers.setdefault(key, WorkerSummary(key))

    for c in connectors:
        finding = ConnectorFinding(connector=c, warnings=_warnings_for(c))
        w = worker(c.worker_id)
        w.connectors += 1
        if c.connector_failed:
            d = classify(c.trace)
            finding.failures.append(Failure(c.name, None, c.worker_id, d))
            w.failed_connectors += 1
            w.categories[d.category.key] += 1
        for t in c.tasks:
            tw = worker(t.worker_id)
            tw.tasks += 1
            if t.state == "FAILED":
                d = classify(t.trace)
                finding.failures.append(Failure(c.name, t.id, t.worker_id, d))
                tw.failed_tasks += 1
                tw.categories[d.category.key] += 1
        findings.append(finding)

    notes: list[str] = []
    for w in workers.values():
        if w.categories.get("oom"):
            notes.append(
                f"{w.worker_id}: {w.categories['oom']} out-of-memory failure(s); every connector on this worker "
                "shares the heap, so restart it with more memory rather than restarting tasks one by one"
            )
    total_failed = sum(w.failed_tasks + w.failed_connectors for w in workers.values())
    if total_failed >= 3:
        for w in workers.values():
            mine = w.failed_tasks + w.failed_connectors
            if mine == total_failed and len(workers) > 1:
                notes.append(f"all {total_failed} failures are on {w.worker_id}: suspect that worker (memory, network, plugins)")
    return Report(
        findings=findings,
        workers=sorted(workers.values(), key=lambda w: w.worker_id),
        worker_notes=notes,
    )


def restart_command(base_url: str, name: str) -> str:
    return (
        "curl -s -X POST -H 'Content-Type: application/json' "
        + shlex.quote(base_url.rstrip("/") + restart_path(name))
    )


def legacy_restart_commands(base_url: str, finding: ConnectorFinding) -> list[str]:
    """Equivalent calls for Connect older than 3.0, which lacks includeTasks/onlyFailed."""
    base = base_url.rstrip("/")
    name = quote(finding.connector.name, safe="")
    cmds = []
    if finding.connector.connector_failed:
        cmds.append(f"curl -s -X POST -H 'Content-Type: application/json' {shlex.quote(f'{base}/connectors/{name}/restart')}")
    for t in finding.connector.failed_tasks:
        cmds.append(
            f"curl -s -X POST -H 'Content-Type: application/json' {shlex.quote(f'{base}/connectors/{name}/tasks/{t.id}/restart')}"
        )
    return cmds
