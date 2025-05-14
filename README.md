# kafka-connect-doctor

Diagnose Kafka Connect connectors from the REST API status: which connectors and tasks failed, why
(classified into plain-English categories from the Java stack trace), how failures spread across
workers, and a restart plan that tells you when a restart is pointless until something is fixed.

**Status:** v0.1. Working today: reading saved REST API JSON, a read-only live fetch with `--url`,
trace classification (authentication, serialization/schema, missing topic, connection/timeout,
configuration, out of memory), per-worker summary, restart plan with exact `curl` commands, and
restarts sent only with `--apply`. Text and JSON output.

## Install

```bash
pip install git+https://github.com/narayana006/kafka-connect-doctor
```

Python 3.10+, no runtime dependencies.

## Quick start

`examples/connectors-expanded.json` is real output of
`GET /connectors?expand=status&expand=info` from a throwaway local Apache Kafka 3.9.0 Connect worker,
with one task broken by a converter mismatch and one by an unusable file path:

```bash
kafka-connect-doctor examples/connectors-expanded.json
```

```text
4 connectors, 3 tasks: 0 failed connectors, 2 failed tasks, 1 connectors with warnings

== audit-archive-sink [FileStreamSinkConnector] (sink) connector RUNNING on localhost:8083, tasks 0/1 running
  audit-archive-sink task 0 FAILED on localhost:8083: Configuration error
    root cause: java.nio.file.NoSuchFileException: /proc/archive/audit.log
    why: The connector configuration is invalid or refers to something that does not exist (missing required setting, bad value, unusable path).
    next: Correct the config (PUT /connectors/{name}/config; check it first with PUT /connector-plugins/{class}/config/validate), then restart the failed tasks.

== dr-mirror-source [MirrorSourceConnector] (source) connector RUNNING on localhost:8083, tasks 0/0 running
  warning: connector is RUNNING but has no tasks: it may still be starting, or it could not generate task configs (for example it cannot reach its source); check the worker log

== orders-file-sink [FileStreamSinkConnector] (sink) connector RUNNING on localhost:8083, tasks 0/1 running
  orders-file-sink task 0 FAILED on localhost:8083: Serialization / schema
    root cause: com.fasterxml.jackson.core.JsonParseException: Unrecognized token 'period': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')
    why: A record could not be converted. The converter does not match how the data was written (e.g. JsonConverter on plain strings, Avro converter on non-Avro bytes) or the schema is missing or incompatible.
    next: Align key.converter/value.converter (and schemas.enable) with the topic's data, or route bad records with errors.tolerance=all (plus a dead letter queue for sink connectors). A restart alone will hit the same record again.

Workers:
  localhost:8083: 4 connectors, 3 tasks, 2 failed (config 1, serialization 1)

Restart plan (Connect 3.0+; restarts only the failed connector/tasks):
  fix-first audit-archive-sink: a restart will likely fail again (Configuration error)
    curl -s -X POST -H 'Content-Type: application/json' 'http://CONNECT_HOST:8083/connectors/audit-archive-sink/restart?includeTasks=true&onlyFailed=true'
  fix-first orders-file-sink: a restart will likely fail again (Serialization / schema)
    curl -s -X POST -H 'Content-Type: application/json' 'http://CONNECT_HOST:8083/connectors/orders-file-sink/restart?includeTasks=true&onlyFailed=true'
```

Exit code 3 means failures were found (0 = none, 2 = bad input, 4 = a restart sent with `--apply`
was rejected).

`examples/illustrative-cluster.json` is a **hand-written, illustrative** three-worker cluster (the
traces imitate real exception messages, the hosts are example.com). The end of its report:

```bash
kafka-connect-doctor examples/illustrative-cluster.json --connect-url http://connect-1.example.com:8083
```

```text
Workers:
  (unassigned): 0 connectors, 1 tasks, 0 failed
  connect-1.example.com:8083: 3 connectors, 3 tasks, 1 failed (auth 1)
  connect-2.example.com:8083: 3 connectors, 5 tasks, 3 failed (auth 1, missing_topic 1, serialization 1)
  connect-3.example.com:8083: 2 connectors, 2 tasks, 3 failed (config 1, connectivity 1, oom 1)
  note: connect-3.example.com:8083: 1 out-of-memory failure(s); every connector on this worker shares the heap, so restart it with more memory rather than restarting tasks one by one

Restart plan (Connect 3.0+; restarts only the failed connector/tasks):
  fix-first audit-source: a restart will likely fail again (Missing topic)
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/audit-source/restart?includeTasks=true&onlyFailed=true'
  restart   inventory-cdc-source
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/inventory-cdc-source/restart?includeTasks=true&onlyFailed=true'
  fix-first legacy-jdbc-source: a restart will likely fail again (Configuration error)
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/legacy-jdbc-source/restart?includeTasks=true&onlyFailed=true'
  fix-first orders-avro-sink: a restart will likely fail again (Serialization / schema)
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/orders-avro-sink/restart?includeTasks=true&onlyFailed=true'
  fix-first pg-orders-sink: a restart will likely fail again (Authentication / authorization)
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/pg-orders-sink/restart?includeTasks=true&onlyFailed=true'
  restart   s3-archive-sink
    curl -s -X POST -H 'Content-Type: application/json' 'http://connect-1.example.com:8083/connectors/s3-archive-sink/restart?includeTasks=true&onlyFailed=true'
```

## Usage

### From saved files (default, offline)

```bash
curl -s 'http://CONNECT_HOST:8083/connectors?expand=status&expand=info' > connect.json
kafka-connect-doctor connect.json
```

Accepted input, one or more files or `-` for stdin:

- `GET /connectors?expand=status&expand=info` (best: includes the connector class)
- `GET /connectors?expand=status`
- `GET /connectors/{name}/status`, or a JSON list of such objects

The `expand` parameter needs Kafka Connect 2.3 or later; on older workers save `/status` per connector.

### Live, read-only

```bash
kafka-connect-doctor --url http://connect-1.example.com:8083
```

This makes one request, `GET /connectors?expand=status&expand=info`, and prints the same report.
Nothing is restarted.

### Applying restarts

```bash
kafka-connect-doctor --url http://connect-1.example.com:8083 --apply
```

With `--apply` the tool sends
`POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true` for each connector whose plan is
`restart`, which restarts only the failed connector instance and failed tasks. Connectors marked
`fix-first` are skipped unless you add `--include-fix-first`. Those query parameters were added in
Apache Kafka 3.0.0 (KIP-745); the request returns `202 Accepted`. For older workers use `--legacy`
to print the per-task `POST /connectors/{name}/tasks/{id}/restart` and
`POST /connectors/{name}/restart` calls instead. The live path was checked against a local Kafka 3.9.0
Connect worker.

| Option | Meaning |
|---|---|
| `--url URL` | fetch live from one worker (any worker answers for the whole cluster) |
| `--apply` | with `--url`: send the restarts in the plan |
| `--include-fix-first` | with `--apply`: also restart connectors whose failures a restart is unlikely to fix |
| `--connect-url URL` | base URL for the printed `curl` commands (default `--url` or `http://CONNECT_HOST:8083`) |
| `--legacy` | also print restart calls for Connect older than 3.0 |
| `--show-healthy` | list healthy connectors too |
| `--timeout SECONDS` | HTTP timeout for `--url` (default 10) |
| `--format json` | machine-readable report |

### Categories

The deepest `Caused by:` in the trace is checked first, so a wrapper like
`Tolerance exceeded in error handler` is explained by its real cause.

| Category | Typical root cause | Restart plan |
|---|---|---|
| Authentication / authorization | `SaslAuthenticationException`, `TopicAuthorizationException`, `SSLHandshakeException`, database password failures, HTTP 401/403 | fix-first |
| Serialization / schema | `SerializationException`, `JsonParseException`, `Unknown magic byte!`, `DataException` from a converter | fix-first (the same record fails again) |
| Missing topic | `UnknownTopicOrPartitionException`, `Topic ... not present in metadata` | fix-first |
| Connection refused / timeout | `java.net.ConnectException`, `UnknownHostException`, `SocketTimeoutException`, `TimeoutException` | restart |
| Configuration error | `ConfigException`, `ClassNotFoundException`, unusable file paths | fix-first |
| Out of memory | `OutOfMemoryError` | restart, plus a note to fix the worker heap |
| Unclassified | anything else | fix-first (read the root cause) |

It also warns about connectors that are `RUNNING` with zero tasks, `UNASSIGNED` connectors or tasks,
`PAUSED`/`STOPPED` connectors, restarts in progress, and failures concentrated on one worker.

## Feature checklist

- [x] Read saved `/connectors?expand=status&expand=info`, `?expand=status` and `/connectors/{name}/status` JSON
- [x] Classify task and connector traces into categories with plain-English explanations and next steps
- [x] Per-worker summary with out-of-memory and single-worker notes
- [x] Restart plan with `includeTasks=true&onlyFailed=true` `curl` commands, plus pre-3.0 equivalents
- [x] Read-only live fetch through an injectable HTTP client; restarts only with `--apply`
- [x] JSON output
- [ ] Authentication options for secured Connect REST endpoints (basic auth, mTLS)
- [ ] Pull the worker log lines around each failure
- [ ] Config checks against `/connector-plugins/{class}/config/validate`

## Limitations

- Classification is pattern matching on exception names and messages. Connector plugins phrase
  errors differently; unknown phrasing falls back to "Unclassified" with the root cause shown.
- "restart" vs "fix-first" is a heuristic. A connection error may be permanent, and an auth error can
  be fixed by rotating a secret outside Connect; read the root cause before acting.
- `--url` sends no credentials, so it only works against REST endpoints without authentication.
  Use saved files (fetched with your usual `curl` options) for secured clusters.
- A status snapshot does not show history; a task that keeps failing and being restarted by
  automation may look healthy at the moment of capture.

## Permissions

Reading saved files needs nothing. `--url` makes a `GET` request, and `--apply` makes `POST` requests,
to the Connect REST API; on a cluster with REST authorization the account needs permission to read
connector status and to restart connectors and tasks.

## Contributing

Issues and pull requests are welcome, especially real (redacted) traces that are classified wrongly.

```bash
pip install -e ".[dev]"
pytest
```

Built and maintained by [Narayana Vemburaj](https://github.com/narayana006).

Licensed under the [MIT License](LICENSE).
