import pytest

from kafka_connect_doctor.model import InputError, load_connectors, merge

from .conftest import load


def test_expanded_status_and_info_real_capture():
    conns = merge(load_connectors(load("connectors-expanded.json")))
    assert [c.name for c in conns] == ["audit-archive-sink", "dr-mirror-source", "orders-file-sink", "shipments-file-sink"]
    orders = conns[2]
    assert orders.type == "sink"
    assert orders.connector_class == "org.apache.kafka.connect.file.FileStreamSinkConnector"
    assert [t.state for t in orders.tasks] == ["FAILED"]
    assert "Tolerance exceeded" in orders.tasks[0].trace


def test_single_status_object():
    (c,) = load_connectors(load("audit-archive-sink.status.json"))
    assert c.name == "audit-archive-sink"
    assert c.config is None
    assert c.failed_tasks[0].id == 0


def test_list_of_status_objects_and_merge_keeps_config():
    expanded = load_connectors(load("connectors-expanded.json"))
    single = load_connectors([load("audit-archive-sink.status.json")])
    merged = merge(expanded + single)
    audit = next(c for c in merged if c.name == "audit-archive-sink")
    assert audit.config is not None


def test_expand_status_only_uses_key_as_fallback_name():
    doc = {"x": {"status": {"connector": {"state": "RUNNING", "worker_id": "w:8083"}, "tasks": []}}}
    (c,) = load_connectors(doc)
    assert c.name == "x" and c.config is None


def test_empty_object_means_no_connectors():
    assert load_connectors({}) == []


def test_info_only_is_explained():
    with pytest.raises(InputError, match="expand=status"):
        load_connectors({"x": {"info": {"config": {}}}})


@pytest.mark.parametrize("doc", [["a", "b"], {"a": 1}, 42, {"name": "x", "connector": {}, "tasks": []}])
def test_rejects_unrecognised(doc):
    with pytest.raises(InputError):
        load_connectors(doc)


def test_task_without_state_rejected():
    with pytest.raises(InputError, match="without id/state"):
        load_connectors({"name": "x", "connector": {"state": "RUNNING"}, "tasks": [{"id": 0}]})
