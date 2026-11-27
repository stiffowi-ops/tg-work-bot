FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod main.go main_test.go ./
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /workhub .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S workhub && adduser -S -G workhub workhub
COPY --from=build /workhub /usr/local/bin/workhub
USER workhub
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/workhub"]
