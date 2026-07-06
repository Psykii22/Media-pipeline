package ffmpeg

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
)

type VideoTask struct {
	ID        string `json:"id"`
	InputURL  string `json:"input_url"`
	OutputKey string `json:"output_key"`
}

type Processor struct {
	OutputDir string
}

func NewProcessor(outputDir string) *Processor {
	return &Processor{OutputDir: outputDir}
}

func (p *Processor) ProcessVideo(ctx context.Context, task VideoTask) ([]byte, error) {
	outputPath := filepath.Join(p.OutputDir, task.OutputKey)

	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return nil, err
	}

	playlist := filepath.Join(outputPath, "playlist.m3u8")
	segments := filepath.Join(outputPath, "segment_%03d.ts")
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y",
		"-i", task.InputURL,

		"-c:v", "libx264",
		"-c:a", "aac",

		"-hls_time", "10",
		"-hls_playlist_type", "vod",

		"-hls_segment_filename", segments,

		"-f", "hls",

		"-threads", "2",

		playlist,
	)
	return cmd.CombinedOutput()
}
