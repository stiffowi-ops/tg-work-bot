FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod *.go ./
COPY web ./web
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /workhub .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S workhub && adduser -S -G workhub workhub
COPY --from=build /workhub /usr/local/bin/workhub
RUN mkdir -p /app/runtime && chown workhub:workhub /app/runtime
USER workhub
ENV PORT=8080 DATA_DIR=/app/runtime
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/workhub"]
