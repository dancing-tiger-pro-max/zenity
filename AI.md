# AI usage

## Setup
- **Tool:** Claude Code (VS Code extension), two sessions: design started in one, continued and implemented in a
  second (Claude Opus 5.5).
- **Configuration:** `CLAUDE.md` in the repo root holds the project rules the agent reads every session: never log
  post text, record every design decision before coding, check NATS API signatures with `go doc`, keep handlers to
  `Handle(ctx, event) error`, all settings in `config.yaml`, meaningful tests over broad ones. A personal
  `CLAUDE.local.md` (not committed) holds working preferences, such as explaining Go/Kubernetes/NATS concepts by
  comparison to Node.js, which is my background.
- **No MCP servers or custom skills.** The agent used the terminal directly (`go`, `docker`, `kind`, `kubectl`) and
  fetched the Jetstream docs when a behavior needed checking.

## How I worked with it
Before involving the agent I sketched a rough design myself (notes and a diagram): one ingester, a broker in the
middle, and separate workers per event type. The agent sessions built on that shape: choosing the broker (NATS
JetStream) and working through delivery, backpressure and scaling. Design first, in a working document, one decision at a time: the agent proposed options with tradeoffs, I asked
questions until I could explain the choice myself, and only then was it recorded. The working log grew to 21
decisions; this repo's `DESIGN.md` is the condensed result. Implementation then went in small steps, each one
runnable and checked before the next.

## Prompts behind the key decisions
1. **Setting the process.** *"Let's start looking at it and list our requirements. Don't do anything yet. I want us
   to create an md design file with all the requirements. We will fill it together slowly."*
2. **Refusing to assume data is disposable.** The agent's first backpressure proposal shed likes under pressure.
   *"Why is it OK to lose likes? What is this data for? We don't know if it's OK to lose this data."* That led to a
   no-loss default, and later to the explicit two-of-three tradeoff in DESIGN.md §4.
3. **Separating "built" from "production".** *"State in the design file what we actually implement, what would be
   better in production and why. Where we're not sure what's best, write down the decision and why."* This became
   the prototype → production table.
4. **Catching a stall in the design.** The no-loss policy meant the ingester would wait on a full stream, and with
   one ingester reading one ordered stream, every path would stall behind the slow one. *"That's a big gap in our
   design: one full queue stops all paths. How can we solve it?"* We switched to per-path eviction with a monitor,
   so a slow path can only hurt itself.
5. **Holding the agent to the brief.** After several rounds on scaling the likes counter (sharding, Redis, batched KV
   counters, counting inside NATS), *"read the assignment and tell me which idea is better based on it."* The agent
   reversed its own recommendation: build the simple exact single-pod counter, describe the scale-out plan.
6. **Full consistency review.** *"Read the assignment and the design. Make sure it meets all requirements, all
   decisions work together, and there are no open decisions."* This found real bugs (a backlog warning that would
   always fire, loss detection that broke with replicas, a window based on the wrong clock) and prompted a live
   test of the Jetstream feed to replace assumptions with measurements.

## Where it helped
- Laying out options with honest costs, and explaining unfamiliar Go, NATS and Kubernetes concepts through Node.js
  equivalents.
- Verifying instead of assuming: it connected to the live feed to measure traffic (~490 events/s), confirm that an
  old cursor is silently clamped to ~36h, and measure the catch-up rate that justified async publishing.
- Cross-checking decisions against each other when one changed.

## Where I overrode it
- **Data loss:** rejected shedding likes "for the demo", and later rejected the stall-everything design that came
  from over-correcting.
- **Exactness:** rejected an approximate engagement counter. Counts had to be exact, so the window uses event time
  and dedupes by URI.
- **Scaling:** rejected sharding the likes worker because it can't autoscale, and kept pushing until the
  single-pod choice was justified by measured volume rather than convenience.
- **Simplicity:** rejected active-active ingesters, and a configurable list of tombstoned collections.
- **Extensibility:** monitor warnings go through the same `Notifier` interface as alerts, so Slack can be added
  later without touching detection logic.
- **Process:** required a runnable checkpoint after every implementation step instead of large batches.
