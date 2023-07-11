package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"

	"github.com/c-bata/go-prompt"
)

var (
	pathsForPrompetr   []string
	isDelveForPrompetr bool
)

func executor(ctx context.Context, s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	} else if s == "quit" || s == "exit" {
		fmt.Println("Bye!")
		os.Exit(0)
	}

	app, err := s3s.New(ctx)
	if err != nil {
		os.Exit(0)
	}

	appForPrompetr := &cli.App{
		Name:    "s3s",
		Version: Version,
		Usage:   "Easy S3 select like searching in directories",
		Commands: []*cli.Command{
			{
				Name: "use",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "delve",
						Usage:       "like directory move before querying",
						Destination: &isDelveForPrompetr,
					},
				},
				Action: func(c *cli.Context) error {
					if isDelveForPrompetr {
						newPaths, err := pathDelver(c.Context, app, c.Args().Slice())
						if err != nil {
							return errors.WithStack(err)
						}
						pathsForPrompetr = newPaths
						return nil
					}
					if c.NArg() == 0 {
						fmt.Println("Please specify a directory path.")
						return nil
					}
					return nil
				},
			},
			{
				Name: "state",
				Action: func(c *cli.Context) error {
					fmt.Println(pathsForPrompetr)
					return nil
				},
			},
			{
				Name:    "select",
				Aliases: []string{"SELECT"},
				Action: func(c *cli.Context) error {
					var queryStrr string
					if c.NArg() > 0 {
						q := []string{"SELECT"}
						q = append(q, c.Args().Slice()...)
						queryStrr = strings.Join(q, " ")
					} else {
						queryStrr = DEFAULT_QUERY
					}
					var outputPath string
					if output != "" {
						f, err := filepath.Abs(output)
						if err != nil {
							log.Println(err)
							return nil
						}
						outputPath = f
					}

					var query *s3s.Query
					switch {
					case isCSV:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeCSV,
							Query:      queryStrr,
						}
					case isALBLogs:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeALBLogs,
							Query:      queryStrr,
						}
						if duration > 0 {
							query.Since = time.Now().UTC().Add(-duration)
							query.Until = time.Now().UTC()
						} else {
							query.Since = since
							query.Until = until
						}
					case isCFLogs:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeCFLogs,
							Query:      queryStrr,
						}
						if duration > 0 {
							query.Since = time.Now().UTC().Add(-duration)
							query.Until = time.Now().UTC()
						} else {
							query.Since = since
							query.Until = until
						}
					default:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeJSON,
							Query:      queryStrr,
						}
					}
					option := &s3s.Option{
						IsDryRun:    isDryRun,
						IsCountMode: isCount,
						Output:      outputPath,
					}

					if _, err := app.Run(ctx, pathsForPrompetr, query, option); err != nil {
						log.Println(err)
						return nil
					}
					return nil
				},
			},
		},
	}

	args := []string{"s3s"}
	args = append(args, strings.Fields(s)...)
	appForPrompetr.RunContext(ctx, args)
}

func completer(d prompt.Document) []prompt.Suggest {
	return []prompt.Suggest{}
}
func prompter(ctx context.Context) error {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program.")
	defer fmt.Println("Bye!")

	p := prompt.New(
		func(s string) {
			executor(ctx, s)
		},
		completer,
		prompt.OptionTitle("s3s: Easy S3 select like searching in directories"),
		prompt.OptionPrefix(">>> "),
	)
	p.Run()
	return nil
}
