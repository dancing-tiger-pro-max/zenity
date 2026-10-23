# One image for every role; the role is chosen with --role at run time.

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /router ./cmd/router

# Static binary on a minimal base: no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /router /router
ENTRYPOINT ["/router", "--config", "/etc/router/config.yaml"]
