# syntax=docker/dockerfile:1

# =============================================================================
# Build stage — compile the requested service binary.
# ARG SERVICE must be one of: ingest-api | consumer-service | replay-service
# =============================================================================
FROM golang:1.24-alpine AS builder

ARG SERVICE
RUN test -n "$SERVICE" || (echo "ARG SERVICE is required" && exit 1)

WORKDIR /app

# Fetch dependencies first to benefit from Docker layer cache.
COPY go.mod go.sum ./
RUN go mod download

# Copy full source tree.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /bin/app \
    ./cmd/${SERVICE}

# =============================================================================
# Runtime stage — minimal image with just the binary.
# =============================================================================
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata wget

RUN addgroup -S app && adduser -S app -G app

COPY --from=builder /bin/app /bin/app

USER app

ENTRYPOINT ["/bin/app"]
