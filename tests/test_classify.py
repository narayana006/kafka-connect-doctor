import pytest

from kafka_connect_doctor.classify import classify, exception_chain

from .conftest import load


def real_trace(name):
    return load("connectors-expanded.json")[name]["status"]["tasks"][0]["trace"]


def test_real_jsonconverter_failure_is_serialization():
    d = classify(real_trace("orders-file-sink"))
    assert d.category.key == "serialization"
    assert d.root_cause.startswith("com.fasterxml.jackson.core.JsonParseException: Unrecognized token")
    assert not d.category.restart_helps


def test_real_file_sink_failure_is_config():
    d = classify(real_trace("audit-archive-sink"))
    assert d.category.key == "config"
    assert d.root_cause == "java.nio.file.NoSuchFileException: /proc/archive/audit.log"


def test_chain_skips_frames_and_more_lines():
    chain = exception_chain(real_trace("orders-file-sink"))
    assert len(chain) == 4
    assert not any(line.startswith("at ") or "more" == line.split()[-1] for line in chain)


@pytest.mark.parametrize(
    "trace,expected",
    [
        ("org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512", "auth"),
        ("org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [orders]", "auth"),
        ("org.apache.kafka.connect.errors.ConnectException: x\nCaused by: javax.net.ssl.SSLHandshakeException: PKIX path building failed", "auth"),
        ("org.apache.kafka.connect.errors.ConnectException: HTTP 401 Unauthorized from https://api.example.com", "auth"),
        ("org.apache.kafka.common.errors.SerializationException: Unknown magic byte!", "serialization"),
        ("org.apache.kafka.connect.errors.DataException: JsonConverter with schemas.enable requires \"schema\" and \"payload\" fields and may not contain additional fields.", "serialization"),
        ("org.apache.kafka.common.errors.UnknownTopicOrPartitionException: This server does not host this topic-partition.", "missing_topic"),
        ("org.apache.kafka.connect.errors.ConnectException: x\nCaused by: org.apache.kafka.common.errors.TimeoutException: Topic a.b not present in metadata after 60000 ms.", "missing_topic"),
        ("org.apache.kafka.connect.errors.ConnectException: x\nCaused by: java.net.ConnectException: Connection refused", "connectivity"),
        ("java.net.UnknownHostException: db.example.com", "connectivity"),
        ("com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure", "connectivity"),
        ("org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before the position for partition x-0 could be determined", "connectivity"),
        ("org.apache.kafka.common.config.ConfigException: Missing required configuration \"connection.url\" which has no default value.", "config"),
        ("org.apache.kafka.common.config.ConfigException: Invalid value abc for configuration batch.size: Not a number of type INT", "config"),
        ("java.lang.ClassNotFoundException: com.example.MissingTransform", "config"),
        ("java.lang.OutOfMemoryError: Java heap space", "oom"),
        ("org.apache.kafka.connect.errors.ConnectException: wrapped\nCaused by: java.lang.OutOfMemoryError: GC overhead limit exceeded", "oom"),
        ("java.lang.IllegalStateException: something odd", "unknown"),
    ],
)
def test_categories(trace, expected):
    assert classify(trace).category.key == expected


def test_root_cause_wins_over_wrapper():
    # wrapper mentions a timeout, root cause is authentication
    trace = (
        "org.apache.kafka.connect.errors.ConnectException: Timed out waiting for the database\n"
        "\tat a.b.C.d(C.java:1)\n"
        "Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"svc\"\n"
        "\t... 3 more\n"
    )
    assert classify(trace).category.key == "auth"


def test_no_trace():
    d = classify(None)
    assert d.category.key == "unknown"
    assert d.root_cause == "(no trace reported)"


def test_offset_number_is_not_http_401():
    assert classify("java.lang.IllegalStateException: bad record at offset 401").category.key == "unknown"


def test_long_root_cause_is_shortened():
    d = classify("java.lang.IllegalStateException: " + "x" * 500)
    assert len(d.root_cause) <= 220 and d.root_cause.endswith("...")
