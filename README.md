# kafka-connect-doctor

Diagnose Kafka Connect connectors from the REST API status: failed tasks, classified errors, and a safe restart plan.

## Planned scope

Read saved JSON from the Connect REST API (/connectors?expand=status&expand=info, or per-connector /status) or fetch it through an injectable HTTP client (tested with a fake; default dry-run, never POSTs unless --apply). Classify task traces into categories (auth, serialization/schema, connection refused/timeouts, missing topic, config errors, OOM) with plain-English explanations, summarise per worker, and print restart commands (POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true — verify these query params exist and since which version before stating it).
