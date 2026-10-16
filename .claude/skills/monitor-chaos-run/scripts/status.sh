#!/usr/bin/env bash
# Usage: status.sh RELEASE
# Prints the Oxia pods, the runner jobs of the latest revision, the chaos workflows, and each
# runner's last checkpoint.
set -u
RELEASE=$1
NS=${NS:-oxia-chaos}
RUNNERS="app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=runner"

date -u +"now %FT%TZ"
kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" -o wide || exit 1
echo "coordinator lease: $(kubectl -n "$NS" get lease "$RELEASE-status" -o jsonpath='{.spec.holderIdentity}')"
REVISION=$(kubectl -n "$NS" get jobs -l "$RUNNERS" -o jsonpath='{range .items[*]}{.metadata.labels.helm\.sh/revision}{"\n"}{end}' | sort -n | tail -1)
if [ -z "$REVISION" ]; then
  echo "no runner jobs"
else
  kubectl -n "$NS" get jobs -l "$RUNNERS,helm.sh/revision=$REVISION"
  for pod in $(kubectl -n "$NS" get pods -l "$RUNNERS,helm.sh/revision=$REVISION" -o name); do
    echo "$pod: $(kubectl -n "$NS" logs "$pod" -c runner --since=10m | grep 'checkpoint passed' | tail -1 | cut -c1-200)"
  done
fi
kubectl -n "$NS" get workflows.chaos-mesh.org 2>/dev/null | grep -E "NAME|$RELEASE"
