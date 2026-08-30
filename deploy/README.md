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

- Oxia 0.16.8;
- Chaos Mesh 2.8.4; and
- the Java runner Job.

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

The default case runs for six hours. Use Helm overrides for a shorter local run,
for example:

```shell
make -C deploy release \
  HELM_ARGS='--set cases.basic-kv.duration=10m --set runnerJob.activeDeadlineSeconds=1200'
```

Delete the local cluster when finished:

```shell
make deploy-down
```
