from kafka_connect_doctor.model import load_connectors, merge
from kafka_connect_doctor.report import diagnose, legacy_restart_commands, restart_command

from .conftest import load


def illustrative():
    return diagnose(merge(load_connectors(load("illustrative-cluster.json"))))


def by_name(report, name):
    return next(f for f in report.findings if f.connector.name == name)


def test_counts():
    r = illustrative()
    assert r.counts == {
        "connectors": 8,
        "tasks": 11,
        "failed_connectors": 1,
        "failed_tasks": 6,
        "connectors_with_warnings": 2,
    }


def test_actions():
    r = illustrative()
    assert by_name(r, "inventory-cdc-source").action == "restart"
    assert by_name(r, "s3-archive-sink").action == "restart"
    assert by_name(r, "pg-orders-sink").action == "fix-first"
    assert by_name(r, "legacy-jdbc-source").action == "fix-first"
    assert by_name(r, "metrics-sink").action == "none"


def test_connector_level_failure_recorded():
    f = by_name(illustrative(), "legacy-jdbc-source")
    assert f.failures[0].task_id is None
    assert f.failures[0].label == "legacy-jdbc-source (connector)"


def test_worker_summary_and_oom_note():
    r = illustrative()
    w3 = next(w for w in r.workers if w.worker_id == "connect-3.example.com:8083")
    assert (w3.failed_tasks, w3.failed_connectors) == (2, 1)
    assert w3.categories["oom"] == 1
    assert any("out-of-memory" in n and "connect-3" in n for n in r.worker_notes)


def test_failures_concentrated_on_one_worker_note():
    doc = {
        "name": "a",
        "connector": {"state": "RUNNING", "worker_id": "w1:8083"},
        "tasks": [{"id": i, "state": "FAILED", "worker_id": "w1:8083", "trace": "java.net.ConnectException: Connection refused"} for i in range(3)]
        + [{"id": 3, "state": "RUNNING", "worker_id": "w2:8083"}],
    }
    r = diagnose(load_connectors(doc))
    assert any("all 3 failures are on w1:8083" in n for n in r.worker_notes)


def test_warnings_real_capture_running_without_tasks():
    r = diagnose(merge(load_connectors(load("connectors-expanded.json"))))
    mirror = by_name(r, "dr-mirror-source")
    assert mirror.failures == [] and "no tasks" in mirror.warnings[0]
    assert by_name(r, "shipments-file-sink").healthy


def test_restart_command_quotes_and_encodes():
    cmd = restart_command("http://c:8083/", "my connector/1")
    assert cmd == (
        "curl -s -X POST -H 'Content-Type: application/json' "
        "'http://c:8083/connectors/my%20connector%2F1/restart?includeTasks=true&onlyFailed=true'"
    )


def test_legacy_commands_per_failed_task():
    f = by_name(illustrative(), "pg-orders-sink")
    cmds = legacy_restart_commands("http://c:8083", f)
    assert [c.rsplit("/", 2)[-2] for c in cmds] == ["0", "1"]
