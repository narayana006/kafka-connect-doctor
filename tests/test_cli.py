import io
import json

import pytest

from kafka_connect_doctor.cli import EXIT_INPUT_ERROR, EXIT_OK, EXIT_PROBLEMS, EXIT_RESTART_FAILED, main
from kafka_connect_doctor.client import STATUS_PATH, ClientError

from .conftest import EXAMPLES, load


class FakeClient:
    def __init__(self, doc, post_status=202, post_error=None):
        self.doc = doc
        self.post_status = post_status
        self.post_error = post_error
        self.gets = []
        self.posts = []

    def get_json(self, path):
        self.gets.append(path)
        return self.doc

    def post(self, path):
        self.posts.append(path)
        if self.post_error:
            raise ClientError(self.post_error)
        return self.post_status, None


def run(capsys, args, client=None):
    code = main(args, client=client)
    out, err = capsys.readouterr()
    return code, out, err


def test_saved_file_text(capsys):
    code, out, _ = run(capsys, [str(EXAMPLES / "connectors-expanded.json")])
    assert code == EXIT_PROBLEMS
    assert "4 connectors, 3 tasks: 0 failed connectors, 2 failed tasks" in out
    assert "orders-file-sink task 0 FAILED on localhost:8083: Serialization / schema" in out
    assert "http://CONNECT_HOST:8083/connectors/orders-file-sink/restart?includeTasks=true&onlyFailed=true" in out
    assert "shipments-file-sink" not in out


def test_show_healthy(capsys):
    _, out, _ = run(capsys, [str(EXAMPLES / "connectors-expanded.json"), "--show-healthy"])
    assert "== shipments-file-sink" in out


def test_healthy_cluster_from_stdin(capsys, monkeypatch):
    doc = {
        "ok": {
            "status": {
                "name": "ok",
                "connector": {"state": "RUNNING", "worker_id": "w:8083"},
                "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w:8083"}],
                "type": "sink",
            }
        }
    }
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(doc)))
    code, out, _ = run(capsys, ["-"])
    assert code == EXIT_OK
    assert "Restart plan: nothing to restart." in out


def test_json_output(capsys):
    code, out, _ = run(capsys, [str(EXAMPLES / "illustrative-cluster.json"), "--format", "json"])
    assert code == EXIT_PROBLEMS
    doc = json.loads(out)
    assert doc["summary"]["failed_tasks"] == 6
    pg = next(c for c in doc["connectors"] if c["name"] == "pg-orders-sink")
    assert pg["action"] == "fix-first"
    assert {f["category"] for f in pg["failures"]} == {"auth"}
    assert doc["applied_restarts"] is None


def test_url_is_read_only_without_apply(capsys):
    client = FakeClient(load("illustrative-cluster.json"))
    code, out, _ = run(capsys, ["--url", "http://connect-1.example.com:8083"], client=client)
    assert code == EXIT_PROBLEMS
    assert client.gets == [STATUS_PATH]
    assert client.posts == []
    assert "Dry run: nothing was restarted" in out
    assert "'http://connect-1.example.com:8083/connectors/" in out


def test_apply_restarts_only_restartable(capsys):
    client = FakeClient(load("illustrative-cluster.json"))
    code, out, _ = run(capsys, ["--url", "http://c:8083", "--apply"], client=client)
    assert code == EXIT_PROBLEMS
    assert client.posts == [
        "/connectors/inventory-cdc-source/restart?includeTasks=true&onlyFailed=true",
        "/connectors/s3-archive-sink/restart?includeTasks=true&onlyFailed=true",
    ]
    assert "inventory-cdc-source: HTTP 202 ok" in out


def test_apply_include_fix_first(capsys):
    client = FakeClient(load("illustrative-cluster.json"))
    run(capsys, ["--url", "http://c:8083", "--apply", "--include-fix-first"], client=client)
    assert len(client.posts) == 6


def test_apply_failure_exit_code(capsys):
    client = FakeClient(load("illustrative-cluster.json"), post_status=409)
    code, out, _ = run(capsys, ["--url", "http://c:8083", "--apply"], client=client)
    assert code == EXIT_RESTART_FAILED
    assert "HTTP 409 FAILED" in out


def test_apply_network_error(capsys):
    client = FakeClient(load("illustrative-cluster.json"), post_error="connection refused")
    code, out, _ = run(capsys, ["--url", "http://c:8083", "--apply", "--format", "json"], client=client)
    assert code == EXIT_RESTART_FAILED
    assert json.loads(out)["applied_restarts"][0]["error"] == "connection refused"


@pytest.mark.parametrize(
    "args,msg",
    [
        (["--apply", "x.json"], "--apply needs --url"),
        (["--url", "http://c:8083", "x.json"], "not both"),
        ([], "give one or more"),
    ],
)
def test_argument_errors(capsys, args, msg):
    code, _, err = run(capsys, args)
    assert code == EXIT_INPUT_ERROR
    assert msg in err


def test_invalid_json_file(capsys, tmp_path):
    f = tmp_path / "bad.json"
    f.write_text("{not json")
    code, _, err = run(capsys, [str(f)])
    assert code == EXIT_INPUT_ERROR
    assert "not valid JSON" in err


def test_get_failure_is_input_error(capsys):
    class Broken(FakeClient):
        def get_json(self, path):
            raise ClientError("GET failed: connection refused")

    code, _, err = run(capsys, ["--url", "http://c:8083"], client=Broken({}))
    assert code == EXIT_INPUT_ERROR
    assert "connection refused" in err
