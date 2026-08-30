# Test environment

This directory provisions a local Kubernetes environment for developing Oxia
chaos cases. Its kind overlay enables the `oxia-cluster` and `chaos-mesh`
dependencies of the main chart with pinned versions.

Prerequisites:

- Docker
- kind
- kubectl
- Helm

Create the cluster and install the test dependencies from the repository root:

```shell
make deploy-up
```

The environment contains one `oxia-chaos` namespace with:

- Oxia 0.16.8;
- Chaos Mesh 2.8.4; and
- the Java runner Helm test.

The environment chart is installed by `make deploy-up`. The runner image must be
built and loaded into kind before executing the Java case:

```shell
helm test oxia-chaos --namespace oxia-chaos --logs
```

Delete the local cluster when finished:

```shell
make deploy-down
```
