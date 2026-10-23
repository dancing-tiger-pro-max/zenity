# AI usage

## Setup
- **Tool:** Claude Code (VS Code extension), two sessions: design started in one, continued and implemented in a
  second (Claude Opus 5.5).
- **Configuration:** [`CLAUDE.md`](CLAUDE.md) holds the project rules the agent reads every session: never log post
  text; don't make design decisions silently; implement in small steps that each end runnable; never block without
  watching for shutdown (and a timeout for NATS); verify against the live feed; a regression test must be shown to
  fail with the bug put back. Several of these rules were added after a bug they would have prevented. A personal
  `CLAUDE.local.md` (not committed) holds working preferences, such as explaining Go, Kubernetes and NATS through
  Node.js equivalents, which is my background.
- **No MCP servers or custom skills.** The agent used the terminal directly (`go`, `docker`, `kind`, `kubectl`, the
  `nats` CLI), read library docs with `go doc`, and fetched the Jetstream docs when a behavior needed checking.

## How I worked with it
Before involving the agent I sketched a rough design myself (notes and a diagram): one ingester, a broker in the
middle, and separate workers per event type. The agent sessions built on that shape: choosing the broker (NATS
JetStream) and working through delivery, backpressure and scaling.

Design came first, in a working document, one decision at a time: the agent proposed options with tradeoffs, I asked
questions until I could explain the choice myself, and only then was it recorded. The working log reached 23
decisions; `DESIGN.md` is the condensed result. Implementation went in small steps; after each one I asked for
evidence that it worked and for an audit against the design before moving on.

## Prompts behind the key decisions
1. **Setting the process.** *"Let's start looking at it and list our requirements. Don't do anything yet. I want us
   to create an md design file with all the requirements. We will fill it together slowly."*
2. **Refusing to assume data is disposable.** The agent's first backpressure proposal shed likes under pressure.
   *"Why is it OK to lose likes? What is this data for? We don't know if it's OK to lose this data."* That led to a
   no-loss default, and later to the explicit two-of-three tradeoff in DESIGN.md §4.
3. **Catching a stall in the design.** The no-loss policy meant the ingester would wait on a full stream, and with
   one ingester reading one ordered stream, every path would stall behind the slow one. *"That's a big gap in our
   design: one full queue stops all paths. How can we solve it?"* We switched to per-path eviction with a monitor,
   so a slow path can only hurt itself.
4. **Holding the agent to the brief.** After several rounds on scaling the likes counter (sharding, Redis, batched KV
   counters, counting inside NATS), *"read the assignment and tell me which idea is better based on it."* The agent
   reversed its own recommendation: build the simple exact single-pod counter, describe the scale-out plan.
5. **Small, observable steps.** *"I want between each step to be able to run the code and see what we currently
   have. I want to make sure small pieces work before moving on."* This turned a 6-step plan into 16 steps, each
   ending in something runnable.
6. **The audit prompt, repeated after most steps.** *"Before moving on, make sure everything works as expected in the
   design file. Are there any gaps? New bugs we didn't think about? Any problem with running our design live?"* This
   is the prompt that found most of the bugs below.
7. **Not accepting the first fix.** After the agent fixed an ingester freeze with a timeout: *"Are you sure this is
   the best solution? How do we know this problem won't keep happening? Is it fully fixed? How can I see it with my
   own eyes?"* That produced a comparison of alternatives, a new CLAUDE.md rule, and a regression test that was
   then shown to fail 5/5 with the bug put back.
8. **Knowing where to stop.** *"I'm thinking not to stop here and not implement, but just mention what we have
   left."* The agent agreed for features (graceful drain, health probes are described, not built) but pushed back
   that the Dockerfile, kind manifests and README are required deliverables, so those were finished.

## Where it helped
- Laying out options with honest costs, and explaining unfamiliar Go, NATS and Kubernetes concepts through Node.js
  equivalents.
- Verifying instead of assuming: it measured the live feed (~460–490 events/s, ~62k events/s on replay), confirmed
  that an old cursor is silently clamped to ~36h, and ran failure tests (NATS down, `kill -9` a worker, a poison
  message) on both the laptop and kind.
- Cross-checking decisions against each other when one changed.

## Where it got things wrong (and how they were caught)
The agent wrote these bugs; running the system live and the audit prompt caught them:
- Used `did:time_us` as the dedupe ID; it isn't unique (a thread is one commit), so ~1.4% of events were dropped.
  Caught by a `duplicates` counter that moved with no restart.
- No timeout on async publishes: with NATS down the ingester froze silently and ignored Ctrl-C.
- A likes replay that read one message per request and took 44s.
- A monitor loss count ~5× too low, caught by comparing it with an independent count.
- A message that could get stuck forever if its dead-letter publish failed.
- Weak evidence: a regression test that passed even with the bug present (until made deterministic), and a NATS
  counter misread as a running total. Each time, the fix was to show the check failing first.

## Where I overrode it
- **Data loss:** rejected shedding likes "for the demo", and later rejected the stall-everything design that came
  from over-correcting.
- **Exactness:** rejected an approximate engagement counter, so the window uses event time and dedupes by URI.
- **Scaling:** rejected sharding the likes worker because it can't autoscale, and kept pushing until the
  single-pod choice was justified by measured volume rather than convenience.
- **Simplicity:** rejected active-active ingesters, and a configurable list of tombstoned collections.
- **Extensibility:** monitor warnings go through the same `Notifier` interface as alerts, so Slack can be added
  later without touching detection logic.
- **Process:** required a runnable checkpoint and evidence after every step, and audits before moving on.
