"""Command-line interface for kafka-connect-doctor."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from . import __version__
from .client import STATUS_PATH, ClientError, ConnectClient, UrllibConnectClient, restart_path
from .model import InputError, load_connectors, merge
from .report import Report, diagnose, legacy_restart_commands, restart_command

EXIT_OK = 0
EXIT_INPUT_ERROR = 2
EXIT_PROBLEMS = 3
EXIT_RESTART_FAILED = 4

DEFAULT_PRINT_URL = "http://CONNECT_HOST:8083"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="kafka-connect-doctor",
        description=(
            "Diagnose Kafka Connect connectors from REST API status JSON: failed tasks, classified errors, "
            "a per-worker summary and a safe restart plan. Reads saved files by default; --url fetches "
            "live (GET only), and restarts are only sent with --apply."
        ),
    )
    p.add_argument("files", nargs="*", help="saved JSON from /connectors?expand=status&expand=info or /connectors/{name}/status ('-' for stdin)")
    p.add_argument("--url", help="fetch status live from this Connect worker (e.g. http://connect-1:8083); read-only unless --apply")
    p.add_argument("--apply", action="store_true", help="with --url: POST restarts for connectors whose plan is 'restart'")
    p.add_argument("--include-fix-first", action="store_true", help="with --apply: also restart connectors whose plan is 'fix-first'")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds for --url (default 10)")
    p.add_argument("--connect-url", help=f"base URL used in printed curl commands (default: --url or {DEFAULT_PRINT_URL})")
    p.add_argument("--legacy", action="store_true", help="also print per-task restart commands for Connect older than 3.0")
    p.add_argument("--show-healthy", action="store_true", help="list healthy connectors too")
    p.add_argument("--format", choices=("text", "json"), default="text")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def _load_files(paths: list[str]) -> list[Any]:
    docs = []
    for path in paths:
        text = sys.stdin.read() if path == "-" else open(path, encoding="utf-8").read()
        try:
            docs.append(json.loads(text))
        except json.JSONDecodeError as exc:
            raise InputError(f"{path}: not valid JSON ({exc})") from exc
    return docs


def render_text(report: Report, base_url: str, show_healthy: bool, legacy: bool) -> str:
    out: list[str] = []
    c = report.counts
    out.append(
        f"{c['connectors']} connectors, {c['tasks']} tasks: "
        f"{c['failed_connectors']} failed connectors, {c['failed_tasks']} failed tasks, "
        f"{c['connectors_with_warnings']} connectors with warnings"
    )
    for f in report.findings:
        if f.healthy and not show_healthy:
            continue
        conn = f.connector
        klass = f" [{conn.connector_class.rsplit('.', 1)[-1]}]" if conn.connector_class else ""
        tasks_running = sum(1 for t in conn.tasks if t.state == "RUNNING")
        out.append("")
        out.append(
            f"== {conn.name}{klass} ({conn.type or 'unknown type'}) connector {conn.state} on {conn.worker_id or '-'}, "
            f"tasks {tasks_running}/{len(conn.tasks)} running"
        )
        for fail in f.failures:
            d = fail.diagnosis
            where = f" on {fail.worker_id}" if fail.worker_id else ""
            out.append(f"  {fail.label} FAILED{where}: {d.category.title}")
            out.append(f"    root cause: {d.root_cause}")
            out.append(f"    why: {d.category.explanation}")
            out.append(f"    next: {d.category.next_step}")
        for w in f.warnings:
            out.append(f"  warning: {w}")
    out.append("")
    out.append("Workers:")
    for w in report.workers:
        cats = ", ".join(f"{k} {v}" for k, v in sorted(w.categories.items()))
        out.append(
            f"  {w.worker_id}: {w.connectors} connectors, {w.tasks} tasks, "
            f"{w.failed_connectors + w.failed_tasks} failed" + (f" ({cats})" if cats else "")
        )
    for note in report.worker_notes:
        out.append(f"  note: {note}")

    planned = [f for f in report.findings if f.action != "none"]
    out.append("")
    if not planned:
        out.append("Restart plan: nothing to restart.")
        return "\n".join(out) + "\n"
    out.append("Restart plan (Connect 3.0+; restarts only the failed connector/tasks):")
    for f in planned:
        if f.action == "restart":
            out.append(f"  restart   {f.connector.name}")
        else:
            cats = sorted({x.diagnosis.category.title for x in f.failures if not x.diagnosis.category.restart_helps})
            out.append(f"  fix-first {f.connector.name}: a restart will likely fail again ({'; '.join(cats)})")
        out.append(f"    {restart_command(base_url, f.connector.name)}")
        if legacy:
            for cmd in legacy_restart_commands(base_url, f):
                out.append(f"    pre-3.0: {cmd}")
    return "\n".join(out) + "\n"


def render_json(report: Report, base_url: str, applied: list[dict[str, Any]] | None) -> str:
    doc = {
        "summary": report.counts,
        "connectors": [
            {
                "name": f.connector.name,
                "type": f.connector.type,
                "class": f.connector.connector_class,
                "state": f.connector.state,
                "worker_id": f.connector.worker_id,
                "tasks": [{"id": t.id, "state": t.state, "worker_id": t.worker_id} for t in f.connector.tasks],
                "failures": [
                    {
                        "task_id": x.task_id,
                        "worker_id": x.worker_id,
                        "category": x.diagnosis.category.key,
                        "root_cause": x.diagnosis.root_cause,
                        "explanation": x.diagnosis.category.explanation,
                        "next_step": x.diagnosis.category.next_step,
                        "restart_helps": x.diagnosis.category.restart_helps,
                    }
                    for x in f.failures
                ],
                "warnings": f.warnings,
                "action": f.action,
                "restart_command": restart_command(base_url, f.connector.name) if f.action != "none" else None,
            }
            for f in report.findings
        ],
        "workers": [
            {
                "worker_id": w.worker_id,
                "connectors": w.connectors,
                "tasks": w.tasks,
                "failed": w.failed_connectors + w.failed_tasks,
                "categories": dict(w.categories),
            }
            for w in report.workers
        ],
        "worker_notes": report.worker_notes,
        "applied_restarts": applied,
    }
    return json.dumps(doc, indent=2) + "\n"


def apply_restarts(client: ConnectClient, report: Report, include_fix_first: bool) -> list[dict[str, Any]]:
    results = []
    for f in report.findings:
        if f.action == "restart" or (f.action == "fix-first" and include_fix_first):
            path = restart_path(f.connector.name)
            try:
                status, _ = client.post(path)
                ok = status in (200, 202, 204)
                results.append({"connector": f.connector.name, "status": status, "ok": ok})
            except ClientError as exc:
                results.append({"connector": f.connector.name, "status": None, "ok": False, "error": str(exc)})
    return results


def main(argv: list[str] | None = None, client: ConnectClient | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply and not args.url:
        print("error: --apply needs --url (saved files cannot be restarted)", file=sys.stderr)
        return EXIT_INPUT_ERROR
    if args.url and args.files:
        print("error: give either saved files or --url, not both", file=sys.stderr)
        return EXIT_INPUT_ERROR
    if not args.url and not args.files:
        print("error: give one or more saved JSON files, '-' for stdin, or --url", file=sys.stderr)
        return EXIT_INPUT_ERROR

    try:
        if args.url:
            client = client or UrllibConnectClient(args.url, timeout=args.timeout)
            docs = [client.get_json(STATUS_PATH)]
        else:
            docs = _load_files(args.files)
        connectors = merge([c for d in docs for c in load_connectors(d)])
    except (InputError, ClientError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INPUT_ERROR

    report = diagnose(connectors)
    base_url = args.connect_url or args.url or DEFAULT_PRINT_URL

    applied = None
    if args.apply:
        assert client is not None
        applied = apply_restarts(client, report, args.include_fix_first)

    if args.format == "json":
        sys.stdout.write(render_json(report, base_url, applied))
    else:
        sys.stdout.write(render_text(report, base_url, args.show_healthy, args.legacy))
        if args.url and not args.apply and any(f.action != "none" for f in report.findings):
            sys.stdout.write("\nDry run: nothing was restarted. Re-run with --apply to send the restarts above.\n")
        if applied is not None:
            sys.stdout.write("\nApplied restarts:\n")
            if not applied:
                sys.stdout.write("  none (no connector had plan 'restart'; see --include-fix-first)\n")
            for r in applied:
                state = "ok" if r["ok"] else "FAILED"
                sys.stdout.write(f"  {r['connector']}: HTTP {r['status']} {state}{' ' + r['error'] if 'error' in r else ''}\n")

    if applied is not None and not all(r["ok"] for r in applied):
        return EXIT_RESTART_FAILED
    if report.failures:
        return EXIT_PROBLEMS
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
