package s3s

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type App struct {
	s3 *s3.Client
}

func NewApp(ctx context.Context, region string, maxRetries int) (*App, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.RetryMaxAttempts = maxRetries
		o.RetryMode = aws.RetryModeStandard
	})

	app := &App{
		s3: client,
	}

	return app, nil
}
