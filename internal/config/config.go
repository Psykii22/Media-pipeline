package config

import "os"

type Config struct {
	BrokerAddress string
	TopicIngest   string
	TopicSuccess  string
	TopicError    string
	ConsumerGroup string
	NumWorkers    int
}

func Load() *Config {
	return &Config{
		BrokerAddress: getEnv("KAFKA_BROKER", "localhost:9092"),
		TopicIngest:   getEnv("TOPIC_INGEST", "video-uploads"),
		TopicSuccess:  getEnv("TOPIC_SUCCESS", "video-processed"),
		TopicError:    getEnv("TOPIC_ERROR", "video-errors"),
		ConsumerGroup: getEnv("CONSUMER_GROUP", "ffmpeg-processors"),
		NumWorkers:    3,
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
