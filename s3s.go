package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type App struct {
	s3client *s3.Client
}

func NewApp(ctx context.Context) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(cfg)
	return &App{s3client: s3Client}, nil
}
