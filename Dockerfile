FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod *.go ./
COPY web ./web
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /workhub .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates su-exec && addgroup -S workhub && adduser -S -G workhub workhub
COPY --from=build /workhub /usr/local/bin/workhub
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh && /bin/sh -n /usr/local/bin/docker-entrypoint.sh && mkdir -p /app/data && chown workhub:workhub /app/data
ENV PORT=8080 DATA_DIR=/app/data
EXPOSE 8080
# Liveness uses the actual runtime port, including host-assigned PORT.
# This endpoint does not require Telegram credentials or network access.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD wget -q -O /dev/null "http://127.0.0.1:${PORT:-8080}/api/health" || exit 1
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
