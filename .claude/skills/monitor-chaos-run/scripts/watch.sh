#!/usr/bin/env bash
# Usage: watch.sh RELEASE [MINUTES]
# Watches a chaos release for up to MINUTES (default 60). Every INTERVAL seconds it archives new
# log lines from the runners, data servers and coordinators, then checks for problems. On the first
# problem it saves a snapshot, prints an ALERT line and exits 1. It prints OK and exits 0 when the
# time is up.
#
# Environment: NS (default oxia-chaos), OUT (default ./chaos-monitor/RELEASE), INTERVAL (30),
# STALL seconds without a runner checkpoint (180), MAX_ATTEMPT retries of one operation (20).
set -u
RELEASE=$1
MINUTES=${2:-60}
NS=${NS:-oxia-chaos}
OUT=${OUT:-./chaos-monitor/$RELEASE}
INTERVAL=${INTERVAL:-30}
STALL=${STALL:-180}
MAX_ATTEMPT=${MAX_ATTEMPT:-20}
SKILL=$(cd "$(dirname "$0")/.." && pwd)
RUNNERS="app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=runner"
# Signatures that need a look every time, even after review.
ALWAYS='correctness violation|runner failed|exhausted its retry window|Election aborted|^(CRASH|FTL|PNC) '

mkdir -p "$OUT/logs" "$OUT/state"
touch "$OUT/state/ended" "$OUT/state/restarts"
KNOWN=$OUT/known-signatures.txt
[ -f "$KNOWN" ] || cp "$SKILL/known-signatures.txt" "$KNOWN"

alert() {
  local snapshot
  snapshot=$("$SKILL/scripts/snapshot.sh" "$RELEASE" "$OUT/snapshot-$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null)
  echo "ALERT $(date -u +%FT%TZ) $*"
  echo "snapshot: ${snapshot:-failed}"
  exit 1
}

# Appends the container's lines newer than the archive's last line, and prints them.
archive() {
  local file=$OUT/logs/$1${2:+-$2}.log last
  last=$(tail -n 1 "$file" 2>/dev/null | cut -d' ' -f1)
  kubectl -n "$NS" logs "$1" ${2:+-c "$2"} --timestamps --since=$((INTERVAL * 2 + 30))s 2>/dev/null \
    | awk -v last="$last" '$1 > last' | tee -a "$file"
}

end=$(($(date +%s) + MINUTES * 60))
while [ "$(date +%s)" -lt "$end" ]; do
  now=$(date +%s)
  if ! servers=$(kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" -o jsonpath='{.items[*].metadata.name}' 2>&1); then
    echo "ALERT $(date -u +%FT%TZ) cluster unreachable: $servers"
    exit 1
  fi
  revision=$(kubectl -n "$NS" get jobs -l "$RUNNERS" -o jsonpath='{range .items[*]}{.metadata.labels.helm\.sh/revision}{"\n"}{end}' | sort -n | tail -1)

  # Runner jobs that ended.
  if [ -n "$revision" ]; then
    ended=$(kubectl -n "$NS" get jobs -l "$RUNNERS,helm.sh/revision=$revision" \
      -o jsonpath='{range .items[*]}{.metadata.name}{range .status.conditions[?(@.status=="True")]} {.type}{end}{"\n"}{end}' \
      | grep -E ' (Failed|Complete)' | grep -vxF -f "$OUT/state/ended")
    if [ -n "$ended" ]; then
      echo "$ended" >> "$OUT/state/ended"
      alert "job ended: $ended"
    fi
  fi

  # Containers that restarted in place. A pod kill recreates the pod instead, so this is not chaos.
  restarts=$(kubectl -n "$NS" get pods -l "oxia_cluster=$RELEASE" \
    -o jsonpath='{range .items[*]}{range .status.containerStatuses[?(@.lastState.terminated)]}{.name} {.lastState.terminated.reason} {.lastState.terminated.finishedAt}{"\n"}{end}{end}')
  new_restarts=$(echo "$restarts" | grep . | grep -vxF -f "$OUT/state/restarts")
  if [ -n "$new_restarts" ]; then
    echo "$new_restarts" >> "$OUT/state/restarts"
    alert "container restarted: $new_restarts"
  fi

  # Archive new lines and turn them into signatures.
  signatures=$(
    for pod in $servers; do
      for container in server coordinator; do
        archive "$pod" "$container" | python3 -I "$SKILL/scripts/logsig.py" "$pod-$container"
      done
    done
    if [ -n "$revision" ]; then
      for pod in $(kubectl -n "$NS" get pods -l "$RUNNERS,helm.sh/revision=$revision" -o jsonpath='{.items[*].metadata.name}'); do
        lines=$(archive "$pod" runner)
        [ -f "$OUT/state/$pod.checkpoint" ] || echo "$now" > "$OUT/state/$pod.checkpoint"
        echo "$lines" | grep -q 'checkpoint passed' && echo "$now" > "$OUT/state/$pod.checkpoint"
        attempt=$(echo "$lines" | grep -oE 'attempt=[0-9]+' | cut -d= -f2 | sort -n | tail -1)
        [ "${attempt:-0}" -ge "$MAX_ATTEMPT" ] && printf '%s\tretry attempt %s\t%s\n' "$pod" "$attempt" \
          "$(echo "$lines" | grep "attempt=$attempt " | tail -1)"
        [ $((now - $(cat "$OUT/state/$pod.checkpoint"))) -ge "$STALL" ] && printf '%s\tstalled\tno checkpoint for %ss\n' \
          "$pod" $((now - $(cat "$OUT/state/$pod.checkpoint")))
        echo "$lines" | python3 -I "$SKILL/scripts/logsig.py" "$pod"
      done
    fi
  )
  [ -z "$signatures" ] && { sleep "$INTERVAL"; continue; }

  urgent=$(echo "$signatures" | awk -F'\t' -v always="$ALWAYS" '$2 ~ always || $2 ~ /^(retry attempt|stalled)/' | head -5)
  [ -n "$urgent" ] && alert "$(printf 'always-alert signature:\n%s' "$(echo "$urgent" | cut -c1-700)")"
  new=$(echo "$signatures" | awk -F'\t' 'NR == FNR {known[$0] = 1; next} !($2 in known) && !seen[$2]++' "$KNOWN" -)
  if [ -n "$new" ]; then
    echo "$new" | cut -f2 >> "$KNOWN"
    alert "$(printf 'new signature:\n%s' "$(echo "$new" | cut -c1-700)")"
  fi
  sleep "$INTERVAL"
done
echo "OK $(date -u +%FT%TZ) nothing new in ${MINUTES}m"
