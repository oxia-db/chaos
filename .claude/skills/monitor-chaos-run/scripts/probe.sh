#!/usr/bin/env bash
# Usage: probe.sh RELEASE
# Writes, reads and deletes probe keys in every Oxia namespace, through the same service address
# the runners use. Refuses to run while a runner is active, because a case that lists or scans its
# namespace could report the probe keys as a correctness violation.
set -u
RELEASE=$1
NS=${NS:-oxia-chaos}
KEYS=${KEYS:-20}

active=$(kubectl -n "$NS" get pods -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=runner" \
  --field-selector=status.phase=Running -o name) || exit 1
if [ -n "$active" ]; then
  echo "runners are active, not probing:"; echo "$active"; exit 1
fi
POD=$(kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" -o jsonpath='{.items[0].metadata.name}') || exit 1
NAMESPACES=$(kubectl -n "$NS" get configmap "$RELEASE-status" -o jsonpath='{.data.status}' \
  | awk '/^namespaces:/ {in_ns=1; next} /^[^ ]/ {in_ns=0} in_ns && /^    [^ ].*:$/ {sub(/^ +/, ""); sub(/:$/, ""); print}')
for namespace in $NAMESPACES; do
  kubectl -n "$NS" exec "$POD" -c server -- sh -c '
    client="oxia client -l error -a $1 -n $2 --request-timeout 10s"
    ok=0; failed=0; start=$(date +%s)
    for i in $(seq 1 $3); do
      key=/chaos-monitor-probe/$i
      if out=$($client put $key v$i 2>&1) && out=$($client get $key 2>&1) && echo "$out" | grep -q "v$i" \
        && out=$($client delete $key 2>&1); then
        ok=$((ok + 1))
      else
        failed=$((failed + 1)); echo "$2 $key: $out" | head -c 300; echo
      fi
    done
    echo "$2: ok=$ok failed=$failed in $(( $(date +%s) - start ))s"' \
    sh "$RELEASE.$NS.svc.cluster.local:6648" "$namespace" "$KEYS"
done
