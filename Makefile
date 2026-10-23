CLUSTER        := bsky-router
NS             := bsky-router
IMAGE          := bsky-router:dev
NATS_IMAGE     := nats:2.11.8
NATS_BOX_IMAGE := natsio/nats-box:0.18.0
NET            := bsky-router
K              := kubectl -n $(NS)

.PHONY: build test up down image config deploy status lag logs alerts slow-likes fast-likes \
        nats-up nats-down nats

# ---------- local development (no Kubernetes) ----------

build:
	go build -o bin/router ./cmd/router

test:
	go test ./...

# Local NATS with JetStream, for running roles on the laptop.
nats-up:
	docker network inspect $(NET) >/dev/null 2>&1 || docker network create $(NET)
	docker run -d --name nats --network $(NET) -p 4222:4222 -p 8222:8222 \
		-v bsky-router-nats:/data $(NATS_IMAGE) -js -sd /data
	@echo "NATS on nats://localhost:4222"

nats-down:
	docker rm -f nats

# Run the nats CLI against local NATS, e.g. make nats ARGS="stream ls"
nats:
	@docker run --rm --network $(NET) $(NATS_BOX_IMAGE) nats -s nats://nats:4222 $(ARGS)

# ---------- kind ----------

# Everything from zero: cluster, image, NATS, config, all roles.
up:
	@kind get clusters | grep -qx $(CLUSTER) || kind create cluster --name $(CLUSTER)
	$(MAKE) image deploy

down:
	kind delete cluster --name $(CLUSTER)

image:
	docker build -t $(IMAGE) .
	kind load docker-image $(IMAGE) --name $(CLUSTER)

# The ConfigMap is config.yaml itself, with the in-cluster NATS address.
# Override a value for a demo with CONFIG_SED, e.g. CONFIG_SED='s/x/y/'.
config:
	@sed -e 's#nats://localhost:4222#nats://nats:4222#' $(if $(CONFIG_SED),-e '$(CONFIG_SED)') config.yaml > /tmp/bsky-router-config.yaml
	@kubectl create configmap router-config -n $(NS) --from-file=config.yaml=/tmp/bsky-router-config.yaml \
		--dry-run=client -o yaml | kubectl apply -f -

deploy:
	kubectl create namespace $(NS) --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deploy/nats.yaml
	$(K) rollout status statefulset/nats --timeout=120s
	$(MAKE) config
	kubectl apply -f deploy/router.yaml
	$(K) rollout status deploy --timeout=120s
	@$(MAKE) -s status

status:
	@$(K) get pods -o wide

# How far behind each path is: waiting (pending) and unacked, per consumer.
lag:
	@for s in POSTS LIKES DELETES; do $(K) exec deploy/nats-box -- nats consumer report $$s 2>/dev/null; done

# Follow one role's logs, e.g. make logs ROLE=worker-likes
logs:
	$(K) logs -f deploy/$(ROLE)

# Recent alerts from every path and the monitor.
alerts:
	@for d in worker-posts worker-likes monitor; do $(K) logs deploy/$$d --since=2m | grep '"ALERT"' | tail -n 5; done

# Slow-path demo: slow only the likes worker, then put it back.
slow-likes:
	$(MAKE) config CONFIG_SED='/^  likes:/,/^  deletes:/s/process_delay: .*/process_delay: 50ms/'
	$(K) rollout restart deploy/worker-likes

fast-likes:
	$(MAKE) config
	$(K) rollout restart deploy/worker-likes
