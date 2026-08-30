# Repository Guidelines

## Project Structure

- `runner-java/` contains the Java 21 chaos runner built with the Gradle wrapper.
- `charts/oxia-chaos/` contains the shared Helm chart for Oxia, Chaos Mesh, and runner workloads.
- `deploy/` contains the local kind deployment workflow.
- Use the root `Makefile` as the language-neutral build entry point.

## Java Conventions

- Declare immutable fields, method parameters, and local variables as `final`, including variables used only inside methods.
- Keep command-line code in `cmd`, test implementations in `testcase`, operation constants in `ops`, inference implementations in `inference`, telemetry in `observability`, shared exceptions in `error`, and reusable helpers in `util`.
- Keep runner CLI options compatible across language implementations. Do not add language-specific public options without updating the shared Helm chart contract.
- Format Java with Spotless and target Java 21.

## Validation

- Run `make java-check` for Java formatting and unit tests.
- Run `make java-e2e-test` for Testcontainers coverage when Docker is available.
- Run `make chart-lint` after changing Helm templates or values.
- Run `make check` when changes span the runner and Helm chart.

## Git

- Use conventional commit prefixes such as `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `ci:`, and `chore:`.
- Never use a `codex/` branch prefix. Use conventional prefixes such as `feat/`, `fix/`, `test/`, or `chore/`.
