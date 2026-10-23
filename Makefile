NATS_IMAGE     := nats:2.11.8
NATS_BOX_IMAGE := natsio/nats-box:0.18.0
NET            := bsky-router

.PHONY: build test nats-up nats-down nats

build:
	go build -o bin/router ./cmd/router

test:
	go test ./...

# Local NATS with JetStream, for running roles on the laptop (before kind).
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
