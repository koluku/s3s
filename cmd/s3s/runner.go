package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/c-bata/go-prompt"
	"github.com/dustin/go-humanize"
	"github.com/koluku/s3s"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type Runner interface {
	Run(context.Context) error
}

type CommandLineRunner struct {
	s3s   *s3s.Client
	state *State
}

func (r *CommandLineRunner) Run(ctx context.Context) error {
	if err := r.state.Validate(); err != nil {
		return errors.WithStack(err)
	}

	if r.state.IsDelve {
		newPaths, err := pathDelver(ctx, r.s3s, r.state.Paths)
		if err != nil {
			return errors.WithStack(err)
		}
		r.state.Paths = newPaths
	}

	// Execution
	var outputPath string
	if r.state.Output != "" {
		newOutputPath, err := filepath.Abs(r.state.Output)
		if err != nil {
			return errors.WithStack(err)
		}
		outputPath = newOutputPath
	}

	query := r.state.newquery()
	option := &s3s.Option{
		IsDryRun:    r.state.IsDryRun,
		IsCountMode: r.state.IsCount,
		Output:      outputPath,
	}

	result, err := r.s3s.Run(ctx, r.state.Paths, query, option)
	if err != nil {
		return errors.WithStack(err)
	}

	// Output
	if r.state.IsDryRun {
		fmt.Printf("file count: %s\n", humanize.Comma(int64(result.Count)))
		fmt.Printf("all scan byte: %s\n", humanize.Bytes(uint64(result.Bytes)))
	}
	if r.state.IsDelve {
		for _, path := range r.state.Paths {
			fmt.Fprintln(os.Stderr, path)
		}
	}

	return nil
}

type PromptRunner struct {
	s3s   *s3s.Client
	cli   *cli.App
	state *State
}

func (r *PromptRunner) Run(ctx context.Context) error {
	r.cli = &cli.App{
		Name:    Name,
		Version: Version,
		Usage:   Usage,
		Commands: []*cli.Command{
			{
				Name: "use",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:        "delve",
						Usage:       "like directory move before querying",
						Destination: &r.state.IsDelve,
					},
				},
				Action: func(c *cli.Context) error {
					if r.state.IsDelve {
						newPaths, err := pathDelver(c.Context, r.s3s, c.Args().Slice())
						if err != nil {
							return errors.WithStack(err)
						}
						r.state.Paths = newPaths
						return nil
					}
					if c.NArg() == 0 {
						fmt.Println("Please specify a directory path.")
						return nil
					}
					r.state.Paths = c.Args().Slice()
					return nil
				},
			},
			{
				Name: "state",
				Action: func(c *cli.Context) error {
					fmt.Println(r.state.Paths)
					return nil
				},
			},
			{
				Name:    "select",
				Aliases: []string{"SELECT"},
				Action: func(c *cli.Context) error {
					var queryStr string
					if c.NArg() > 0 {
						q := []string{"SELECT"}
						q = append(q, c.Args().Slice()...)
						queryStr = strings.Join(q, " ")
					} else {
						queryStr = DEFAULT_QUERY
					}
					var outputPath string
					if r.state.Output != "" {
						f, err := filepath.Abs(r.state.Output)
						if err != nil {
							log.Println(err)
							return nil
						}
						outputPath = f
					}

					var query *s3s.Query
					switch {
					case r.state.IsCSV:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeCSV,
							Query:      queryStr,
						}
					case r.state.IsAlbLogs:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeALBLogs,
							Query:      queryStr,
						}
						if r.state.Duration > 0 {
							query.Since = time.Now().UTC().Add(-r.state.Duration)
							query.Until = time.Now().UTC()
						} else {
							if r.state.Since != nil {
								query.Since = *r.state.Since
							} else {
								query.Since = r.state.Until.Add(-r.state.Duration)
							}

							if r.state.Until != nil {
								query.Until = *r.state.Until
							} else {
								query.Until = r.state.Until.Add(r.state.Duration)
							}
						}
					case r.state.IsCfLogs:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeCFLogs,
							Query:      queryStr,
						}
						if r.state.Duration > 0 {
							query.Since = time.Now().UTC().Add(-r.state.Duration)
							query.Until = time.Now().UTC()
						} else {
							if r.state.Since != nil {
								query.Since = *r.state.Since
							} else {
								query.Since = r.state.Until.Add(-r.state.Duration)
							}

							if r.state.Until != nil {
								query.Until = *r.state.Until
							} else {
								query.Until = r.state.Until.Add(r.state.Duration)
							}
						}
					default:
						query = &s3s.Query{
							FormatType: s3s.FormatTypeJSON,
							Query:      queryStr,
						}
					}
					option := &s3s.Option{
						IsDryRun:    r.state.IsDryRun,
						IsCountMode: r.state.IsCount,
						Output:      outputPath,
					}

					p := r.state.Paths
					if _, err := r.s3s.Run(ctx, p, query, option); err != nil {
						log.Println(err)
						return nil
					}
					return nil
				},
			},
		},
	}

	return r.prompter(ctx)
}

func (r *PromptRunner) prompter(ctx context.Context) error {
	fmt.Println("Please use `exit` or `Ctrl-D` to exit this program.")
	defer fmt.Println("Bye!")

	p := prompt.New(
		func(s string) {
			r.executor(ctx, s)
		},
		r.completer,
		prompt.OptionTitle(fmt.Sprintf("%s: %s", Name, Version)),
		prompt.OptionPrefix(">>> "),
	)
	p.Run()
	return nil
}

func (r *PromptRunner) executor(ctx context.Context, s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	} else if s == "quit" || s == "exit" {
		fmt.Println("Bye!")
		os.Exit(0)
	}

	args := []string{Name}
	args = append(args, strings.Fields(s)...)
	r.cli.RunContext(ctx, args)
}

func (r *PromptRunner) completer(d prompt.Document) []prompt.Suggest {
	return []prompt.Suggest{}
}
