# Media Pipeline

Go-based HLS video transcoding pipeline powered by Kafka and FFmpeg. Consumes video jobs from Kafka, transcodes to HLS with libx264, and publishes results back to Kafka.

## Architecture

```
                     ┌──────────────────────┐
                     │    Kafka Cluster      │
                     │  3 Brokers, RF=3     │
                     └──────┬───────────────┘
                            │ topic: video-uploads
                            ▼
               ┌────────────────────────┐
               │   Worker Pool (3 goros) │
               │  Consume → FFmpeg HLS  │
               │  Publish → success/err  │
               └──────┬─────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
  video-processed           video-errors
  (success topic)           (error topic)
```

## Quick Start

```sh
docker compose up --build -d
```

This starts:
- **Zookeeper** — broker coordination
- **Kafka-1,2,3** — 3-broker cluster with replication factor 3
- **Worker** — HLS transcoder
- **Kafka CLI** — helper for manual messages

## Usage

Publish a transcode job:

```sh
echo '{"id":"demo","input_url":"/input/video.mp4","output_key":"demo"}' | \
  docker compose exec -T kafka-cli \
  kafka-console-producer --bootstrap-server kafka-1:9092 --topic video-uploads
```

Watch the worker:

```sh
docker compose logs -f worker
```

HLS output appears in `./output/<output_key>/`.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `TOPIC_INGEST` | `video-uploads` | Source topic for video jobs |
| `TOPIC_SUCCESS` | `video-processed` | Success topic |
| `TOPIC_ERROR` | `video-errors` | Error topic |
| `CONSUMER_GROUP` | `ffmpeg-processors` | Consumer group ID |
| `OUTPUT_DIR` | `./output` | Base directory for HLS output |
| `DEV_MODE` | `false` | Enable test seeder |

## Message Format

```json
{
  "id": "vid-1",
  "input_url": "/input/video.mp4",
  "output_key": "vid-1"
}
```

## Observability (with Minikube)

If you have Loki + Grafana in Minikube:

```sh
# Terminal 1: tunnel Loki to host
kubectl port-forward -n monitoring svc/loki 3100:3100

# Terminal 2: start Promtail
docker compose up -d promtail
```

Query in Grafana: `{app="media-pipeline"}`
