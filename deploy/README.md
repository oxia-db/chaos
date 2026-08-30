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

The default profile runs for six hours. Use the chart-defined quick profile for
a shorter local run:

```shell
make -C deploy up WORKLOAD_PROFILE=quick
```

Delete the local cluster when finished:

```shell
make deploy-down
```
