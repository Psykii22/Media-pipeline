FROM golang:1.26-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/media-worker ./cmd/worker

FROM alpine:3.20

RUN apk add --no-cache ffmpeg

RUN addgroup -S media && adduser -S media -G media
COPY --from=builder /app/media-worker /usr/local/bin/media-worker

USER media

ENTRYPOINT ["media-worker"]
