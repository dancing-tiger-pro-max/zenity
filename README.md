# Bluesky event router

Reads the live Bluesky firehose (Jetstream), routes each event by type through NATS JetStream, and fans out to
independent workers on Kubernetes: **posts** (keyword/language alerts), **likes + reposts** (rolling engagement
alerts), and **deletes** (tombstones). A monitor warns before a slow path loses data and reports any loss.

Design, decisions and tradeoffs: [DESIGN.md](DESIGN.md). Agent setup and prompts: [AI.md](AI.md).

## Prerequisites

| Tool | Tested with | Needed for |
|---|---|---|
| Docker (running) | 28.0 | building the image, running kind |
| [kind](https://kind.sigs.k8s.io) | 0.33 | the local cluster |
| kubectl | 1.32 | talking to it |
| make | any | the commands below |
| Go | 1.27 | only for tests or running without Kubernetes (the image builds Go inside Docker) |

Internet access is needed: the ingester connects to `wss://jetstream2.us-east.bsky.network`.

## Run it

```bash
make up        # creates the kind cluster, builds and loads the image, deploys NATS and every role (~1-2 min)
make status    # 7 pods Running: nats, nats-box, ingester, worker-posts, worker-likes, worker-deletes, monitor
```

`make down` deletes the cluster.

## Watch it work

```bash
make lag                          # per path: messages waiting / unacked. Healthy = 0 / 0
make alerts                       # recent alerts from posts, likes and the monitor
make logs ROLE=ingester           # or worker-posts, worker-likes, worker-deletes, monitor
```

Every role logs JSON. What to look for:
- `ingester`, `"ingest stats"` every 5s: events/s, per-subject counts, `stored`, `duplicates`, `confirmed_lag_ms`
  (how far behind live the safely-stored point is).
- `worker-*`, `"worker stats"` every 10s: `processed`, `event_lag_ms`, `retried`, `dead_lettered`.
- `"ALERT"` lines: `post_match` (keyword or language hit, about one a second with the default `fr` language),
  `engagement` (a post got 20+ likes/reposts within a minute), and from the monitor `backlog_warning`,
  `events_lost`. Alerts carry URIs, DIDs and the matched rule, never post text.
- `monitor`, `"status"` every 30s: waiting messages and unread share per path.

## Demos

**One slow path doesn't affect the others** (the core of the design):
```bash
make slow-likes     # likes worker sleeps 50ms per message (~20/s vs ~350/s arriving)
make lag            # run a few times: LIKES "waiting" grows; POSTS and DELETES stay at 0
make fast-likes     # back to normal: it rebuilds its counts and drains the backlog in seconds
```

**Scale a stateless path:** replicas share one consumer, so NATS splits the work.
```bash
kubectl -n bsky-router scale deploy/worker-posts --replicas=3
make logs ROLE=worker-posts     # each pod's "processed" is about a third
```

**Kill a worker:** Kubernetes replaces it; its unacked messages are redelivered; nothing else notices.
```bash
kubectl -n bsky-router delete pod -l app=worker-deletes
make lag
```

**A poison message goes to the dead-letter stream** (and the worker keeps going):
```bash
kubectl -n bsky-router exec deploy/nats-box -- nats pub bsky.post.create 'not json'
make logs ROLE=worker-posts                                        # "moved to dead-letter stream"
kubectl -n bsky-router exec deploy/nats-box -- nats stream get DLQ 1   # the message, with Error/Attempts headers
```

**Ingester restart resumes from its cursor:**
```bash
kubectl -n bsky-router rollout restart deploy/ingester
make logs ROLE=ingester         # "resuming from saved cursor", then a fast catch-up
```

## Configuration

All settings are in [config.yaml](config.yaml) (comment on every line), validated at startup: unknown keys and
unsafe combinations stop the program with a clear message. To change a value on the cluster:
```bash
make config                                              # push config.yaml to the ConfigMap
kubectl -n bsky-router rollout restart deploy/worker-posts   # restart only the role that uses it
```
Check a config locally with `go run ./cmd/router --check-config`.

## Tests

```bash
go test -race ./...
```
Covers the routing table, the likes window, the worker loop against a real (embedded) NATS, the monitor's
arithmetic, config rules, and a regression test for an ingester freeze. See DESIGN.md §8 for what was verified
live.

## Run without Kubernetes

```bash
make nats-up                      # NATS in Docker on localhost:4222
make build
./bin/router --role=ingester      # one terminal per role
./bin/router --role=worker --type=posts    # or likes, deletes
./bin/router --role=monitor
./bin/router --role=ingester --dry-run     # routing only, no NATS: per-subject counts from the live feed
make nats ARGS="consumer report LIKES"     # lag, via the nats CLI
```

## Assumptions

- Jetstream v1 (`jetstream2.us-east`, the host named in the brief), resumed with its `time_us` cursor.
- Downstream work is simulated: alerts are JSON log lines behind a `Notifier` interface.
- Demo values (1-minute window, threshold 20, `fr` language, small stream limits on a laptop) are chosen so things
  happen while you watch; production values are discussed in DESIGN.md.
- Built: posts, likes/reposts, deletes, monitor. Described only: follows, identity/account, graceful drain, health
  probes, autoscaling, a NATS cluster (DESIGN.md §9).

## Layout

```
cmd/router/          one binary; --role=ingester | worker --type=... | monitor | setup
internal/router      routing table (event type -> subject)
internal/ingest      Jetstream connection, publish, cursor, reconnect, gap detection
internal/broker      streams, KV buckets, cursor and tombstone stores
internal/worker      shared delivery loop: ack, retry, dead-letter, panics
internal/handlers    posts, likes, deletes
internal/engagement  rolling window logic for likes
internal/monitor     backlog warning and loss detection
internal/notify      Notifier interface (log implementation)
internal/config      config.yaml loading and validation
deploy/              Kubernetes manifests
```
