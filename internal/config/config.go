package config

import (
	"os"
	"strings"
)

type Config struct {
	Brokers       []string
	TopicIngest   string
	TopicSuccess  string
	TopicError    string
	ConsumerGroup string
	OutputDir     string
	NumWorkers    int
}

func Load() *Config {
	return &Config{
		Brokers:       splitEnv(getEnv("KAFKA_BROKERS", "localhost:9092")),
		TopicIngest:   getEnv("TOPIC_INGEST", "video-uploads"),
		TopicSuccess:  getEnv("TOPIC_SUCCESS", "video-processed"),
		TopicError:    getEnv("TOPIC_ERROR", "video-errors"),
		ConsumerGroup: getEnv("CONSUMER_GROUP", "ffmpeg-processors"),
		OutputDir:     getEnv("OUTPUT_DIR", "./output"),
		NumWorkers:    3,
	}
}

func splitEnv(val string) []string {
	parts := strings.Split(val, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			res = append(res, s)
		}
	}
	return res
}
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
