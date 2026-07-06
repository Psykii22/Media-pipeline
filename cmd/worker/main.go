package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"media-pipeline/internal/config"
	"media-pipeline/internal/ffmpeg"
	"media-pipeline/internal/queue"
)

func main() {
	log.Println("Starting Production Media Pipeline Worker...")
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize components
	proc := ffmpeg.NewProcessor(cfg.OutputDir)
	successProd := queue.NewProducer(cfg.Brokers, cfg.TopicSuccess)
	errorProd := queue.NewProducer(cfg.Brokers, cfg.TopicError)
	consumer := queue.NewConsumer(cfg.Brokers, cfg.TopicIngest, cfg.ConsumerGroup)

	defer successProd.Close()
	defer errorProd.Close()

	if os.Getenv("DEV_MODE") == "true" {
		go testSeedKafkaQueue(ctx, cfg.Brokers, cfg.TopicIngest)
	}

	jobsChan := make(chan kafka.Message, 10)
	var wg sync.WaitGroup

	// Spawn worker pool
	// keep the workers hanging fan out
	for w := 1; w <= cfg.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, jobsChan, proc, consumer, successProd, errorProd)
		}(w)
	}

	go consumer.StartConsumeLoop(ctx, jobsChan)

	<-ctx.Done()
	log.Println("Gracefully stopping worker pool. Draining channel queues...")
	close(jobsChan)

	wg.Wait()
	log.Println("Pipeline completely shut down.")
}

func runWorker(ctx context.Context, id int, jobs <-chan kafka.Message, proc *ffmpeg.Processor, cons *queue.Consumer, success *queue.Producer, failure *queue.Producer) {
	for msg := range jobs {
		var task ffmpeg.VideoTask
		if err := json.Unmarshal(msg.Value, &task); err != nil {
			log.Printf("[Worker %d] Malformed JSON payload: %v", id, err)
			continue
		}

		log.Printf("[Worker %d] Ingestion accepted for Task: %s", id, task.ID)

		output, err := proc.ProcessVideo(ctx, task)
		if err != nil {
			log.Printf("[Worker %d] Processing failure context: %s. Error: %v", id, string(output), err)
			_ = failure.PublishEvent(ctx, task.ID, err.Error())
			continue
		}

		log.Printf("[Worker %d] Job verified success: %s", id, task.ID)
		_ = success.PublishEvent(ctx, task.ID, "Transcoded 720p match verified.")

		if err := cons.Commit(ctx, msg); err != nil {
			log.Printf("[Worker %d] Offset tracking validation commit failed: %v", id, err)
		}
	}
}

func testSeedKafkaQueue(ctx context.Context, brokers []string, topic string) {
	time.Sleep(3 * time.Second)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	log.Println("[Test Ingestor] Seeding 3 video transcode tasks into Kafka...")

	task := ffmpeg.VideoTask{
		ID:        fmt.Sprintf("vid-%d", 1),
		InputURL:  "input.mp4",
		OutputKey: "vid-1",
	}

	body, err := json.Marshal(task)
	if err != nil {
		log.Printf("[Test Ingestor] JSON Marshalling error: %v", err)
		return
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(task.ID),
		Value: body,
	})

	if err != nil {
		log.Printf("[Test Ingestor] Failed to write message to Kafka: %v", err)
	}

}
