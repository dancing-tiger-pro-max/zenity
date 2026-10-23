# CLAUDE.md

Take-home for a Senior SWE role: a Go service that reads the Bluesky Jetstream firehose, routes each event by
type through NATS JetStream, and fans out to independent worker Deployments on a local kind cluster.

## Files
- `DESIGN.md`: the design deliverable (1–3 pages). Keep it in sync with the code; when a decision changes,
  update it in the same change.
- `AI.md`: agent setup and the prompts behind key decisions.
- `README.md`: exact build, deploy and demo steps.

## Workflow
- Don't make design decisions silently. Propose, wait for an answer, then record the decision.
- When a change affects earlier decisions, check them all and point out conflicts.
- Implement in small steps. Each step must end with something runnable and observable; stop after each step,
  say exactly what to run and what to expect, and wait before moving on.

## Scope
- Build: ingester, posts (keyword/language match), likes/reposts (rolling engagement count + threshold), deletes
  (tombstones), monitor. Everything else is described in `DESIGN.md`, not built.
- Judge additions against the brief: routing, isolation, backpressure and slow paths are what's graded, and so is
  knowing where to stop.

## Code rules
- Never log or emit post text. Alerts carry only URIs, DIDs and the matched rule.
- Layout: `cmd/router` (one binary, `--role`), packages under `internal/` (config, event, router, ingest, broker,
  worker, handlers, notify, monitor), manifests in `deploy/`.
- Each path only implements `Handle(ctx, event) error`; delivery mechanics (fetch, delay, ack, retry, dead-letter)
  live once in `internal/worker`.
- All settings live in `config.yaml`, validated at startup. No hardcoded thresholds or URLs.
- Use `github.com/nats-io/nats.go/jetstream` (the new API), not the legacy `JetStreamContext`. Check signatures with
  `go doc` before using a function.
- Tests: meaningful over broad. Router and likes window logic first.
- Logs: `log/slog` JSON.
