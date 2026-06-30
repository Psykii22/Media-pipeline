package queue

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(broker, topic, group string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			GroupID:  group,
			Topic:    topic,
			MaxBytes: 10e6,
		}),
	}
}

func (c *Consumer) StartConsumeLoop(ctx context.Context, jobsChan chan<- kafka.Message) {
	defer c.reader.Close()
	log.Println("Kafka Consumer loop running...")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("Error fetching Kafka message: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		select {
		case jobsChan <- msg:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}
