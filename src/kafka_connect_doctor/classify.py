"""Classify Kafka Connect failure traces into plain-English categories.

A Connect trace is a Java stack trace: a first line "ExceptionClass: message",
"\\tat ..." frames, and "Caused by: ..." sections. The most specific
information is usually in the deepest "Caused by", so rules are tried from
the root cause upwards and the first match wins.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Category:
    key: str
    title: str
    explanation: str
    restart_helps: bool  # will a plain restart plausibly fix it?
    next_step: str


CATEGORIES: dict[str, Category] = {
    c.key: c
    for c in (
        Category(
            "auth",
            "Authentication / authorization",
            "The task could not authenticate to, or is not authorized on, Kafka or the external system "
            "(wrong or expired credentials, missing ACL/grant, or TLS trust problem).",
            False,
            "Fix the credentials, ACLs or truststore in the connector or worker config, then restart.",
        ),
        Category(
            "serialization",
            "Serialization / schema",
            "A record could not be converted. The converter does not match how the data was written "
            "(e.g. JsonConverter on plain strings, Avro converter on non-Avro bytes) or the schema is "
            "missing or incompatible.",
            False,
            "Align key.converter/value.converter (and schemas.enable) with the topic's data, or route bad "
            "records with errors.tolerance=all (plus a dead letter queue for sink connectors). A restart alone will hit the same record again.",
        ),
        Category(
            "missing_topic",
            "Missing topic",
            "A topic the task reads or writes does not exist (or is not visible to this principal).",
            False,
            "Create the topic or fix topics/topic names in the config, then restart.",
        ),
        Category(
            "connectivity",
            "Connection refused / timeout",
            "The task could not reach a broker, database or endpoint: connection refused, DNS failure, "
            "or a timeout. Often transient (failover, deploy, network blip).",
            True,
            "Check the target is up and reachable from the worker, then restart the failed tasks.",
        ),
        Category(
            "config",
            "Configuration error",
            "The connector configuration is invalid or refers to something that does not exist "
            "(missing required setting, bad value, unusable path).",
            False,
            "Correct the config (PUT /connectors/{name}/config; check it first with "
            "PUT /connector-plugins/{class}/config/validate), then restart the failed tasks.",
        ),
        Category(
            "oom",
            "Out of memory",
            "The worker JVM ran out of memory. Other connectors on the same worker are likely affected too.",
            True,
            "Raise the worker heap (KAFKA_HEAP_OPTS) or lower batch/buffer sizes, then restart. "
            "Restarting without a change may only buy time.",
        ),
        Category(
            "unknown",
            "Unclassified",
            "The trace did not match a known pattern; read the root cause below.",
            False,
            "Read the root cause and the worker log around the failure time before restarting.",
        ),
    )
}

# (category, pattern). Checked against each exception line, root cause first.
RULES: list[tuple[str, re.Pattern[str]]] = [
    ("oom", re.compile(r"OutOfMemoryError|Java heap space|GC overhead limit exceeded|Direct buffer memory")),
    ("auth", re.compile(
        r"SaslAuthenticationException|AuthenticationException|AuthorizationException|"
        r"SSLHandshakeException|password authentication failed|Access denied for user|"
        r"\b401 Unauthorized|\b403 Forbidden|[Ss]tatus [Cc]ode:? 40[13]\b|InvalidAccessKeyId|"
        r"SignatureDoesNotMatch|AccessDenied",
    )),
    ("missing_topic", re.compile(
        r"UnknownTopicOrPartitionException|not present in metadata|Topic .{0,200} does not exist|"
        r"InvalidTopicException",
    )),
    ("serialization", re.compile(
        r"SerializationException|JsonParseException|Unknown magic byte|"
        r"Converting byte\[\] to Kafka Connect data failed|Schema not found|"
        r"JsonConverter with schemas\.enable|Error retrieving (Avro|JSON|Protobuf) schema|"
        r"incompatible with|DataException",
    )),
    ("connectivity", re.compile(
        r"Connection refused|ConnectException: Connection|SocketTimeoutException|UnknownHostException|"
        r"NoRouteToHostException|Communications link failure|Connection reset|TimeoutException|"
        r"timed out|Broken pipe",
    )),
    ("config", re.compile(
        r"ConfigException|Missing required configuration|Invalid value .* for configuration|"
        r"Couldn't find or create file|ClassNotFoundException|NoClassDefFoundError|"
        r"Failed to find any class that implements",
    )),
]

_MORE = re.compile(r"^\.\.\. \d+ more$")
_EXC_LINE = re.compile(r"^(?:Caused by: )?([\w$.]+(?:Exception|Error|Throwable)\b.*)$")


@dataclass(frozen=True)
class Diagnosis:
    category: Category
    root_cause: str  # deepest "Exception: message" line
    matched: str | None  # the exception line that matched a rule


def exception_chain(trace: str) -> list[str]:
    """Return the exception lines, outermost first, skipping stack frames.

    Message continuation lines are appended to their exception line.
    """
    chain: list[str] = []
    for raw in trace.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("at ") or _MORE.match(stripped):
            continue  # stack frame, Jackson "at [Source: ...]" location, or "... N more"
        m = _EXC_LINE.match(stripped)
        if m:
            chain.append(m.group(1))
        elif chain:
            chain[-1] = f"{chain[-1]} {stripped}"
    return chain


def classify(trace: str | None) -> Diagnosis:
    if not trace:
        return Diagnosis(CATEGORIES["unknown"], "(no trace reported)", None)
    chain = exception_chain(trace) or [trace.strip().splitlines()[0]]
    for line in reversed(chain):
        for key, pattern in RULES:
            if pattern.search(line):
                return Diagnosis(CATEGORIES[key], _short(chain[-1]), _short(line))
    return Diagnosis(CATEGORIES["unknown"], _short(chain[-1]), None)


def _short(line: str, limit: int = 220) -> str:
    line = " ".join(line.split())
    return line if len(line) <= limit else line[: limit - 3] + "..."
