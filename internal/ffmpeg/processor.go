package ffmpeg

import (
	"context"
	"os/exec"
	"path/filepath"
)

type VideoTask struct {
	ID        string `json:"id"`
	InputURL  string `json:"input_url"`
	OutputKey string `json:"output_key"`
}
type Processor struct{}

func NewProcessor() *Processor {
	return &Processor{}
}
func (p *Processor) ProcessVideo(ctx context.Context, task VideoTask) ([]byte, error) {
	playlist := filepath.Join(task.OutputKey, "playlist.m3u8")
	segments := filepath.Join(task.OutputKey, "segment_%03d.ts")
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
