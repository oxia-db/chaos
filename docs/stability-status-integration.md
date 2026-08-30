# Stability status integration guide

This guide is the handoff for connecting the Oxia Chaos results to the official
Stability Status page introduced by
[oxia-db.github.io#37](https://github.com/oxia-db/oxia-db.github.io/pull/37).
The website scaffold intentionally contains no example results. Until this
integration publishes its first result, all 90 days are displayed as `not_run`.

## Ownership and endpoint

The `oxia-db/chaos` repository must own and publish the result data. Do not give
the chaos workflow a token that can push to the website repository.

Publish the JSON as the chaos repository's GitHub Pages project site:

```text
https://oxia-db.github.io/chaos/status/v1/summary.json
```

Keep generated history on a dedicated `status-data` branch, then deploy the
same generated directory with GitHub Pages Actions in the publishing workflow.
Do not rely on a `GITHUB_TOKEN` push to `status-data` to trigger another Pages
workflow; pushes made with that token do not start most follow-on workflows.

References:

- [Using custom workflows with GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
- [Automatic token authentication](https://docs.github.com/en/actions/concepts/security/github_token)
- [`actions/deploy-pages`](https://github.com/actions/deploy-pages)

## Public result contract

Use one summary document so the static website needs one request. The document
must contain exactly 90 UTC calendar days for every channel and test case,
oldest first. On the first publication, backfill dates before the first real run
as `not_run`.

The only result values are:

- `passed`: the testcase completed successfully and all required chaos
  injections were observed.
- `failed`: the scheduled testcase started but correctness, runner,
  infrastructure, or chaos-pipeline validation failed. Include a concise title
  and explanation for the website tooltip.
- `not_run`: no result exists for that date, channel, and testcase. This covers
  dates before status publication and cases or server channels that are not yet
  enabled.

Do not publish `passRate`. It is derived by the website as:

```text
passed / (passed + failed)
```

`not_run` entries appear in the history but are excluded from that calculation.
If there are no completed runs, the website displays `No completed runs`.

Use this versioned schema:

```json
{
  "schemaVersion": 1,
  "generatedAt": "2026-08-31T06:15:00Z",
  "windowDays": 90,
  "channels": [
    {
      "id": "stable",
      "serverVersion": "0.16.8",
      "updatedAt": "2026-08-31T06:15:00Z",
      "testCases": [
        {
          "id": "basic-kv",
          "history": [
            {
              "date": "2026-08-30",
              "result": "not_run"
            },
            {
              "date": "2026-08-31",
              "result": "failed",
              "title": "Final state did not match the reference model",
              "detail": "The runner reported a correctness violation after the final recovery checkpoint.",
              "runUrl": "https://github.com/oxia-db/chaos/actions/runs/123456789"
            }
          ]
        }
      ],
      "chaos": {
        "profile": "five-cycle",
        "duration": "6h",
        "expectedInjections": 50,
        "injections": [
          { "type": "pod-kill", "count": 30 },
          { "type": "network-partition", "count": 5 },
          { "type": "cpu-pressure", "count": 5 },
          { "type": "memory-pressure", "count": 5 },
          { "type": "network-latency", "count": 5 }
        ]
      }
    }
  ]
}
```

The example above demonstrates the schema only. Generated production data must
contain the complete 90-day history and all required channels and test cases.

Channel IDs and tested server versions are:

- `stable`: the official `0.16.8` release.
- `beta`: the official `0.17.1` release.

The source tags and displayed fallback versions are defined once in
`config/oxia-channels.json`. Update that file when a channel advances; both the
correctness deployment plan and status publisher consume it.

Never report only a range such as `0.16.x` in JSON. Publish the exact version or
commit that actually ran.

Testcase IDs are:

- `basic-kv`
- `ephemeral`
- `notification`
- `sequences`
- `secondary-index`
- `versioning`

At the time of this handoff, only `basic-kv` is implemented by the Java runner.
Publish the other cases as `not_run` until their jobs genuinely exist.

## Workflow design

The chaos repository has three top-level CI workflows with separate ownership:

1. `ci-docker-release.yaml` builds and publishes runner images.
2. `ci-correctness.yaml` runs quick checks for pull requests and pushes, and the
   six-hour five-cycle profile at `02:00 UTC` each day, after Oxia's scheduled
   `main` image refresh. Each run creates one kind cluster, installs Chaos Mesh
   once, and installs one Oxia Helm release per selected channel. Every enabled
   runner/testcase pair comes from the chart and runs against each channel.
   CI invokes the deployment as a unit instead of enumerating testcases. An
   `if: always()` collector writes per-channel/testcase JSON files and uploads
   them together with `actions/upload-artifact@v7`.
3. `ci-stability-status.yaml` is triggered after a scheduled Correctness run.
   It downloads the source run's result artifacts with
   `actions/download-artifact@v8`, merges the 90-day history, validates it,
   commits it to `status-data`, and deploys the same JSON with GitHub Pages.

The status workflow also supports a manual source run ID for recovering a
publication without rerunning the six-hour test. Automatic publication accepts
scheduled Correctness runs only. Quick results never affect stability history.

For end-to-end validation, manually dispatch Correctness with the
`status-smoke` profile. It runs the short pipeline for both channels and
uploads an isolated `status-smoke-result-all` artifact. The status workflow then
downloads those artifacts, merges and validates a temporary 90-day document,
and packages a Pages artifact. It deliberately does not update `status-data` or
deploy GitHub Pages, so smoke results never enter the public stability history.

If the implemented Stable/basic-kv job fails to upload its result, the publisher
records that scheduled run as `failed` with an artifact error. Unsupported
channels and test cases remain `not_run`; absence of a job is never treated as a
failure.

Keep write permissions job-scoped. The test jobs need only `contents: read`.
The Pages deployment job needs `pages: write` and `id-token: write`, and should
target the `github-pages` environment.

A shortened status workflow shape is:

```yaml
on:
  workflow_run:
    workflows: [Correctness]
    types: [completed]

jobs:
  publish-status:
    if: ${{ github.event.workflow_run.event == 'schedule' }}
    permissions:
      actions: read
      contents: write
    steps:
      - uses: actions/download-artifact@v8
        with:
          pattern: status-result-*
          path: status-results
          merge-multiple: true
          github-token: ${{ github.token }}
          run-id: ${{ github.event.workflow_run.id }}
      - name: Merge and validate the 90-day status document
        run: |
          python3 scripts/status/status.py merge \
            --existing status-data/status/v1/summary.json \
            --results status-results \
            --output status-data/status/v1/summary.json \
            --fallback-date "$RUN_DATE" \
            --fallback-run-url "$RUN_URL"
      - name: Persist generated history
        working-directory: status-data
        run: |
          git config user.name github-actions[bot]
          git config user.email 41898282+github-actions[bot]@users.noreply.github.com
          git add status/v1/summary.json
          git diff --cached --quiet || git commit -m "chore: update stability status"
          git push
      - name: Prepare Pages artifact
        run: |
          mkdir -p pages/status/v1
          cp status-data/status/v1/summary.json pages/status/v1/summary.json
          touch pages/.nojekyll
      - uses: actions/upload-pages-artifact@v5
        with:
          path: pages

  deploy-status:
    if: ${{ always() && needs.publish-status.result == 'success' }}
    needs: [publish-status]
    permissions:
      pages: write
      id-token: write
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v5
```

The action majors above were verified against their official releases on
August 31, 2026.

## Website integration

In `oxia-db/oxia-db.github.io`, update
`src/app/status/status-dashboard.jsx` to fetch the summary URL client-side. The
site is statically exported, so do not depend on a server-only runtime fetch.

The integration must:

- validate `schemaVersion === 1`, the two channel IDs, testcase IDs, dates,
  and the `passed | failed | not_run` result enum;
- sort and keep the latest 90 UTC days, padding absent dates as `not_run`;
- calculate pass rate in the browser instead of trusting a JSON percentage;
- show failure `title` and `detail` in the existing hover/focus tooltip;
- show the exact `serverVersion` and the document's `updatedAt` value;
- treat a fetch or schema error as a data-loading error, not as a failed Oxia
  test; and
- retain the current empty `not_run` state when no status document has ever
  been published.

GitHub Pages currently returns `Access-Control-Allow-Origin: *`, so the official
website can fetch the project-site JSON directly. Verify that header and the
browser request again after the first deployment.

## Acceptance checklist

- A scheduled run publishes even when a testcase job fails.
- The public endpoint returns valid JSON and exactly 90 UTC dates per case.
- Dates before the first real run are `not_run`, not fabricated passes.
- Only completed runs affect the calculated pass rate.
- Failures include a professional, actionable explanation and workflow link.
- The JSON records the exact Oxia server version, not only the release channel.
- The five-cycle pipeline reports 30 pod kills and five each of network
  partition, CPU pressure, memory pressure, and network latency.
- No personal access token or cross-repository write permission is required.
- `make check` passes after runner or chart changes.
