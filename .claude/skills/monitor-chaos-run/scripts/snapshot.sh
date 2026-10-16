#!/usr/bin/env bash
# Usage: snapshot.sh RELEASE DIR
# Saves what is needed to explain a problem before the next pod kill deletes it: coordinator
# status and lease, pods, events, chaos objects, recent logs, metrics, and pprof goroutine dumps.
set -u
RELEASE=$1
OUT=$2
NS=${NS:-oxia-chaos}
mkdir -p "$OUT"

kubectl -n "$NS" get configmap "$RELEASE-status" -o jsonpath='{.data.status}' > "$OUT/status.yaml"
kubectl -n "$NS" get lease "$RELEASE-status" -o yaml > "$OUT/lease.yaml"
kubectl -n "$NS" get pods,jobs -o wide > "$OUT/resources.txt"
kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" -o yaml > "$OUT/pods.yaml"
kubectl -n "$NS" get events --sort-by=.lastTimestamp > "$OUT/events.txt"
kubectl -n "$NS" get workflows.chaos-mesh.org,workflownodes.chaos-mesh.org,podchaos.chaos-mesh.org \
  -o wide > "$OUT/chaos.txt" 2>&1

for pod in $(kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" -o jsonpath='{.items[*].metadata.name}'); do
  for container in server coordinator; do
    kubectl -n "$NS" logs "$pod" -c "$container" --timestamps --since=15m > "$OUT/$pod-$container.log" 2>&1
    kubectl -n "$NS" logs "$pod" -c "$container" --timestamps --previous \
      > "$OUT/$pod-$container-previous.log" 2>/dev/null || rm -f "$OUT/$pod-$container-previous.log"
  done
  # Both containers share the pod network, so the server container's wget reaches either one.
  # Coordinator metrics answer only on the lease holder; pprof only with oxia-cluster.pprofEnabled.
  for target in metrics-server:8080/metrics metrics-coordinator:8081/metrics \
    goroutines-server:6060/debug/pprof/goroutine?debug=2 \
    goroutines-coordinator:6061/debug/pprof/goroutine?debug=2; do
    kubectl -n "$NS" exec "$pod" -c server -- wget -qO- -T 5 "http://127.0.0.1:${target#*:}" \
      > "$OUT/$pod-${target%%:*}.txt" 2>/dev/null || rm -f "$OUT/$pod-${target%%:*}.txt"
  done
done

for pod in $(kubectl -n "$NS" get pods -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=runner" -o jsonpath='{.items[*].metadata.name}'); do
  kubectl -n "$NS" logs "$pod" -c runner --timestamps --since=15m > "$OUT/$pod.log" 2>&1
done
echo "$OUT"
