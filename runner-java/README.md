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
second, executes at most 100 operations per scheduling cycle, and performs a
full checkpoint every minute. Override these settings with:

- `--duration=<Nms|Ns|Nm|Nh>`
- `--key-count=<count>`
- `--rate=<operations-per-second>`; zero disables rate limiting
- `--batch-size=<count>`
- `--checkpoint-interval=<Nms|Ns|Nm|Nh>`

The Oxia namespace is derived internally as `oc-java-basic-kv`.

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
