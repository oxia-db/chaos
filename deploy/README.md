# Test environment

This directory provisions a local Kubernetes environment for developing Oxia
chaos cases. The main chart enables the `oxia-cluster` and `chaos-mesh`
dependencies with pinned versions by default.

Prerequisites:

- Docker
- kind
- kubectl
- Helm
- jq

Create the cluster and install the test dependencies from the repository root:

```shell
make deploy-up
```

The environment contains one `oxia-chaos` namespace with:

- one shared Chaos Mesh 2.8.4 installation;
- one Oxia cluster for every selected server channel; and
- one runner Job for every enabled runner/testcase pair on each channel.

Local deployment selects only the stable channel by default. Set
`CHANNELS='stable beta'` to reproduce the scheduled CI topology. Channel
image sources are defined in `config/oxia-channels.json`; testcase enablement
and workload parameters are defined in `charts/oxia-chaos/values.yaml`.

The environment chart is installed by `make deploy-up`. The runner image is
built and loaded into kind before the Java case starts. The pinned Oxia and
Chaos Mesh images are also pulled through host Docker and loaded into kind.
The local deployment overrides the chart's `Always` pull policy because the
runner image is already loaded into kind. This keeps local loopback HTTP proxy
settings out of the kind nodes. Wait for the correctness workflow, then wait
for the runner and print its logs with:

```shell
make -C deploy correctness-test
make -C deploy test
```

CI sets `PRELOAD_DEPENDENCY_IMAGES=false`, allowing kind to pull immutable
image references directly. This preserves OCI digest names that would
otherwise be rewritten during a host Docker-to-containerd import.

The default profile runs for six hours. Use the chart-defined quick profile for
a shorter local run:

```shell
make -C deploy up WORKLOAD_PROFILE=quick
```

The six-hour profile injects a single pod kill. To keep killing one random
Oxia server every five minutes for the whole six-hour workload, select the
six-hour-periodic chaos profile:

```shell
make -C deploy up CHAOS_PROFILE=six-hour-periodic
```

Override `chaosWorkflow.periodicPodKill.interval` through `HELM_ARGS` to change
the interval. After each kill, the workflow waits until every server is healthy
again and then for the interval, so a run keeps at most one server down. The correctness check
requires at least `EXPECTED_CHAOS_INJECTIONS` injections, 1 by default, and
the periodic profile records one per kill.

The pipeline chaos profile runs a list of fault stages in order. For its
duration, a stage repeats rounds of one fault, a wait until every server is
healthy and the stage's interval, like the periodic profile. After each stage,
the workflow waits for a recovery period and for every server to be healthy
before the next stage starts. The default pipeline kills a whole server pod
every five minutes for the first half of the run, then only the server
container:

```shell
make -C deploy up CHAOS_PROFILE=pipeline
```

Edit `chaosWorkflow.pipeline` in the chart values to change the stages. A
stage's `fault` is `pod-kill`, `container-kill` or `network-partition`.
`container-kill` also takes a `container`, `server` or `coordinator`.
`network-partition` cuts one server pod off the network for its `partition`
duration, such as `1m`, and then heals it. The chart rejects a pipeline
that lasts longer than any enabled case. The correctness check records one
injection per fault.

Set `runnerJob.continuous=true` to run every case without end: each case runs
in a StatefulSet of one pod with `--duration=infinite` and checks every
checkpoint until it's stopped, instead of a Job that ends after the profile's
duration. The fault pipeline then isn't limited by the cases' duration.

Delete the local cluster when finished:

```shell
make deploy-down
```
