package main

import (
	"os"

	"github.com/urfave/cli/v2"
)

const (
	GLOBAL_FLAG_TYPE_CONFIGURE string = "configure"
)

type CliGlobalOption struct {
	Region      string
	ThreadCount int
	MaxRetries  int
	Debug       bool
}

var GlobalOption = &CliGlobalOption{}

var Command = &cli.App{
	Name:    "s3s",
	Version: Version,
	Usage:   "Easy S3 select like searching in directories",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Category:    GLOBAL_FLAG_TYPE_CONFIGURE,
			Name:        "region",
			Usage:       "region of target s3 bucket exist",
			Value:       os.Getenv("AWS_REGION"),
			DefaultText: "ENV[\"AWS_REGION\"]",
			Destination: &GlobalOption.Region,
		},
		&cli.BoolFlag{
			Category:    GLOBAL_FLAG_TYPE_CONFIGURE,
			Name:        "debug",
			Usage:       "erorr check for developper",
			Value:       false,
			Destination: &GlobalOption.Debug,
		},
		&cli.IntFlag{
			Category:    GLOBAL_FLAG_TYPE_CONFIGURE,
			Name:        "thread-count",
			Aliases:     []string{"t, thread_count"},
			Usage:       "max number of api requests to concurrently",
			Value:       DEFAULT_THREAD_COUNT,
			Destination: &GlobalOption.ThreadCount,
		},
		&cli.IntFlag{
			Category:    GLOBAL_FLAG_TYPE_CONFIGURE,
			Name:        "max-retries",
			Aliases:     []string{"M, max_retries"},
			Usage:       "max number of api requests to retry",
			Value:       DEFAULT_MAX_RETRIES,
			Destination: &GlobalOption.MaxRetries,
		},
	},
	Commands: []*cli.Command{
		SubcommandJSON,
		SubcommandCSV,
		SubcommandALB,
		SubcommandCF,
	},
}
