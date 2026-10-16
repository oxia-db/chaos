# CLAUDE.md

This file provides guidance to coding agents working with code in this repository. `AGENTS.md` is a symlink to this file, so agents that look for either name get the same guidance.

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
- Follow the structure of `BasicKv` for new test cases: one public case class wired in `OxiaChaosRunner`, with everything specific to the case next to it. A case with more than one file gets its own package, such as `testcase/ephemeral/`. Keep case logic out of `cmd`.
- Share code between cases through `util` instead of copying it, for example retry policies.
- Inline code as much as possible. Extract a method only when it spans many lines and has several callers.
- Bound every wait on Oxia, a child process, or the network with a timeout.

## Correctness Checks

- A write that fails has an unknown outcome: the server may have applied it, and the Java client (0.10.1 and later) does not resend it. When a case retries a write that is not idempotent, such as a delete, the retry must accept either outcome. The first attempt must still match the reference model exactly.
- The Java client (0.10.1 and later) sends the writes of a shard in submission order over one write stream, so an attempt that timed out is applied before its retry or not at all, even though the client request outlives the case's attempt timeout. Cases may rely on this order within a shard, but not on any order between shards.
- Report a violation only for a result that no valid history can explain.

## Validation

- Run `make java-check` for Java formatting and unit tests.
- Run `make java-e2e-test` for Testcontainers coverage when Docker is available.
- Run `make chart-lint` after changing Helm templates or values.
- Run `make check` when changes span the runner and Helm chart.

## Shared Clusters

- Work only in the namespace you were given, and never touch production clusters.
- Never uninstall Helm releases, delete PVCs, or change other namespaces or the shared Chaos Mesh installation.
- Report unexpected environment errors and correctness violations as issues in `oxia-db/chaos`, with the image digests, run ID, seed, and a timeline from the logs.

## Git

- Use conventional commit prefixes such as `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `ci:`, and `chore:`.
- Never use a `codex/` branch prefix. Use conventional prefixes such as `feat/`, `fix/`, `test/`, or `chore/`.
- Sign off every commit (`git commit -s`); the DCO check requires it.
- Stage the paths you changed instead of `git add -A`, so build and IDE output such as `runner-java/bin/` stays out of commits.
- Keep one change per pull request, and describe it under `## Motivation`, `## Modifications`, and `## Testing`.
- Do not change `runner-java/README.md` unless the change asks for it.
