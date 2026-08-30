# Java runner

The Java runner is a self-contained Java 17 Gradle project using `oxia-client`.

## Build

From the repository root:

```shell
make java-build
```

Alternatively, invoke its Gradle wrapper directly:

```shell
./runner-java/gradlew -p runner-java build
```

## Run

Until the shared case configuration is implemented, the bootstrap entry point
exercises versioned-register create, read, and compare-and-set operations against
an Oxia cluster:

```shell
make run-java ARGS='--service-address=localhost:6648'
```

Use `--namespace=<name>` to select a namespace other than `default`.
