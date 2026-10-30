---
name: monitor-chaos-run
description: Monitor a running oxia-chaos release on Kubernetes. Watches the runner jobs, the Oxia data servers and the coordinators, archives their logs so killed pods keep them, snapshots the cluster on every alert, and triages what it finds. Use when asked to monitor, watch or check a chaos run, or whether an Oxia chaos cluster is healthy.
---

# Monitor a chaos run

A chaos run kills Oxia pods on purpose, so most warnings are expected. The job is to tell the
expected ones from a real problem, keep the evidence before the next kill deletes it, and report.

The scripts take the Helm release name (for example `oxia-chaos-beta`) and read the namespace from
`NS` (default `oxia-chaos`). They need `kubectl` and `python3`. If your cluster needs a wrapper
around `kubectl` (short-lived tokens, a jump host), run the whole script inside it, so each run
gets fresh credentials:

```bash
<wrapper> -- bash .claude/skills/monitor-chaos-run/scripts/watch.sh oxia-chaos-beta 60
```

## Loop

1. Run `scripts/status.sh RELEASE` and report what is running: Oxia pods, runner jobs, the chaos
   workflow, and each runner's last checkpoint.
2. Start `scripts/watch.sh RELEASE MINUTES` in the background. It checks every `INTERVAL` seconds
   (default 30) and exits with an `ALERT` line on the first problem, after saving a snapshot.
   It prints `OK` and exits 0 when the time is up with nothing found.
3. When it exits, report the result to the user, triage the alert (below), and start it again
   with the same `OUT` directory so the archive and the reviewed signatures carry over. A runner
   that stays broken on a filed issue would alert every time; pass its pod name in `IGNORE` (a
   regex) to keep archiving its log without alerting on it.
4. Report status after every step, not only at the end. The user should never have to ask whether
   you are still watching.

`watch.sh` alerts on:

| Alert | Meaning |
|---|---|
| `cluster unreachable` | `kubectl` failed. Credentials probably expired. Never treat this as "no new logs". |
| `job <name> Failed` / `Complete` | A runner ended. Read its log for `correctness violation` or `runner failed`. |
| `container restarted` | A container restarted in place, for example `OOMKilled`. A pod kill recreates the pod instead, so this is not chaos. When the chaos kills containers in place (`container-kill`), set `RESTART_ALERT=OOMKilled` so only the restarts it did not cause alert. |
| `runner restarted` | A runner container restarted. Runners without end (`runnerJob.continuous`) run in StatefulSets and restart only when they fail, so read the previous log with `kubectl logs --previous`. |
| `stalled` | A runner logged no `checkpoint passed` for `STALL` seconds (default 180). |
| `retry attempt N` | An operation was retried `MAX_ATTEMPT` times (default 20, about 1.5 minutes): a shard is unavailable for longer than a failover should take. |
| `always-alert signature` | A line that always needs a look, such as `correctness violation` or `Election aborted`. |
| `new signature` | A warning or error not in the reviewed list. It is added to `OUT/known-signatures.txt` so it alerts once. |

With a fast kill schedule, a known short problem can fire on every few kills and stop the
watcher each time. Then drop its pattern from `ALWAYS` and lower `MAX_ATTEMPT` so a long version of
it still alerts. For example, with a pod kill every 2 minutes, a short `Election aborted` (#19)
follows most coordinator failovers, while 12 retries (about 30 seconds) still catch a long outage:

```bash
MAX_ATTEMPT=12 ALWAYS='correctness violation|runner failed|exhausted its retry window|^(CRASH|FTL|PNC) ' \
  bash .claude/skills/monitor-chaos-run/scripts/watch.sh RELEASE 30
```

Count the dropped signature from `OUT/logs` at the end of the run and report it.

## Evidence

- `OUT` defaults to `./chaos-monitor/RELEASE`. Keep it outside the repository.
- `OUT/logs/` keeps every container's log with timestamps, appended each interval. A pod kill
  deletes the pod's logs, so this archive is often the only copy. It can miss the last interval
  before a kill.
- `OUT/snapshot-*/` is written on every alert by `scripts/snapshot.sh`, which you can also run by
  hand: coordinator status and lease, pods, events, chaos objects, recent logs, metrics, and pprof
  goroutine dumps.
- pprof only answers when the release sets `oxia-cluster.pprofEnabled: true`. The data server
  listens on `127.0.0.1:6060` and the coordinator on `127.0.0.1:6061`, so read both through
  `kubectl exec <pod> -c server -- wget -qO- ...`. Ask for it before a long run: a goroutine dump
  taken while a shard is stuck is the best evidence for a coordinator bug.

## Triage

- **Which coordinator acts.** Every Oxia pod runs a coordinator, but only the holder of the lease
  `RELEASE-status` acts. Read its logs first. Only the lease holder serves metrics on `:8081`.
- **Shard state.** The coordinator keeps each shard's term, leader and ensemble in the configmap
  `RELEASE-status` (key `status`). A term that stays the same while clients get `node is not
  leader` means the coordinator has not started an election.
- **Timeline.** Line up the pod kills (pod start times, `chaos.txt` in the snapshot) with the
  runner retries and the coordinator log. Most problems start within seconds of a kill.
- **Expected during kills:** client retries, keep-alive and health-check failures, `FenceNewTerm`
  or `Replicate` failures toward the killed pod, truncating a follower after a leader change, a
  short burst of attempt timeouts. They are in `known-signatures.txt`.
- **Known open issues.** Check `oxia-db/chaos` issues before filing. For example, a follower
  rejected for missing features after a restart (#7), elections aborted for missing features
  (#19), and a shard left without a working leader after a fast leader restart (#21).
- **Client errors.** The data servers accept only host names from their shard assignments. A
  client that uses a short name such as `RELEASE:6648` gets `PermissionDenied: unexpected
  authority`, which the CLI reports as `failed to retrieve the initial list of shard assignments`.
  Use `RELEASE.NS.svc.cluster.local:6648`.
- **Availability.** `scripts/probe.sh RELEASE` writes, reads and deletes probe keys in every
  namespace. It refuses to run while a runner is active, because the probe keys could look like a
  correctness violation to a case that lists or scans its namespace.

## Reporting

- File unexpected environment errors and correctness violations in `oxia-db/chaos`, following
  [CLAUDE.md](../../../CLAUDE.md): image digests, run ID, a timeline table, the analysis, what is
  missing, and the expected behaviour. Attach the snapshot paths you used.
- Say which logs were lost, if any. An issue that says what we could not see is more useful than
  one that guesses.
- Only watch and report. Do not upgrade, uninstall or delete anything without the user's go-ahead.
