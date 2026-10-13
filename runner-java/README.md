# Java runner

The Java 21 runner executes seeded correctness cases with `oxia-client`.
Each run keeps its reference state in memory, isolates its keys with a unique
prefix, and removes those keys after the final checkpoint.

## Build

From the repository root:

```shell
make java-build
```

Run the Oxia 0.16.8 Testcontainers end-to-end test with Docker available:

```shell
make java-e2e-test
```

Build the runner image with the shared `oxia-chaos` entrypoint:

```shell
make java-image
```

Every language image uses this same entrypoint and CLI contract; Helm changes
the image without changing the workload arguments.

## Run basic-kv

```shell
make run-java ARGS='--case=basic-kv --service-address=localhost:6648'
```

The default workload uses 10,000 keys for 10 minutes at 258 operations per
second, chooses a write, read, or delete-range batch of up to 100 operations per
round, executes that batch concurrently, and performs a full checkpoint every
minute. Override these settings with:

- `--duration=<Nms|Ns|Nm|Nh>`
- `--key-count=<count>`
- `--rate=<operations-per-second>`; zero disables rate limiting
- `--batch-size=<count>`
- `--checkpoint-interval=<Nms|Ns|Nm|Nh>`

The Oxia namespace is derived internally as `oc-java-basic-kv`.

## Run ephemeral

```shell
make run-java ARGS='--case=ephemeral --service-address=localhost:6648'
```

The ephemeral case checks that an ephemeral record lives exactly as long as the
client session that owns it. The runner starts two owner child processes from
its own image and classpath, each holding ephemeral records for half of
`--key-count` keys, and verifies them with a separate observer client. Every
two minutes one owner is ended, alternating between the two, either by closing
its client or by `SIGKILL`, and then replaced. The case fails when:

- a record disappears while the session that wrote it is still alive
- a crashed owner's records disappear before half of the 30-second session
  timeout
- an ended owner's records remain longer than two session timeouts plus 30
  seconds
- a record reports a value, session, or owner different from the last write

When a record is missing, the owner rewrites it to learn its current session.
A new session means the old one legitimately expired during a long fault; it is
counted in `oxia.chaos.session.replacements` instead of failing the run.
`--batch-size` is accepted for CLI compatibility and ignored by this case. The
Oxia namespace is `oc-java-ephemeral`.

## Observability

Logs include the generated seed, run ID, workload configuration, checkpoint
results, and final outcome. OpenTelemetry metrics are available for Prometheus
at `http://0.0.0.0:9464/metrics` by default. Runner and Oxia client traces share
the same OpenTelemetry SDK and can be exported with standard `OTEL_*`
environment variables, for example:

```shell
OTEL_TRACES_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 \
make run-java ARGS='--case=basic-kv --service-address=localhost:6648'
```

Use `--help` to view the complete CLI contract.
