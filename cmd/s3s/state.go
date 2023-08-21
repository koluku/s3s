package main

import (
	"time"

	"github.com/pkg/errors"
)

type State struct {
	// Runner
	Prefixes []string

	// S3 Select Query
	Query   string
	Where   string
	Limit   int
	IsCount bool

	IsCSV     bool
	IsAlbLogs bool
	IsCfLogs  bool

	Duration time.Duration
	Since    *time.Time
	Until    *time.Time

	Output string

	// command option
	IsDelve  bool
	IsDryRun bool
}

func (state *State) Validate() error {
	if err := state.checkArgs(state.Prefixes); err != nil {
		return errors.WithStack(err)
	}
	if err := state.checkTime(); err != nil {
		return errors.WithStack(err)
	}
	if err := state.checkQuery(); err != nil {
		return errors.WithStack(err)
	}
	if err := state.checkFileFormat(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (state *State) checkArgs(paths []string) error {
	if state.IsDelve {
		if len(paths) > 1 {
			return errors.Errorf("too many argument error")
		}
	} else {
		if len(paths) == 0 {
			return errors.Errorf("no argument error")
		}
	}

	return nil
}

var (
	ErrMinusDuration     = errors.New("minus Duration")
	ErrNoTimeOption      = errors.New("no time option")
	ErrOverTimeOption    = errors.New("over time option")
	ErrInvalidTimeOption = errors.New("invalid time option")
)

func (state *State) checkTime() error {
	now := time.Now().UTC()
	if state.IsAlbLogs || state.IsCfLogs {
		if state.Duration < 0 {
			return ErrMinusDuration
		}

		if state.Duration == 0 && state.Since == nil && state.Until == nil {
			return ErrNoTimeOption
		}

		if state.Since != nil && now.Before(*state.Since) {
			return ErrOverTimeOption
		}
		if state.Duration > 0 && state.Until != nil && now.Before(state.Until.Add(-state.Duration)) {
			return ErrOverTimeOption
		}

		if state.Since != nil && state.Until != nil {
			if state.Duration > 0 {
				return ErrInvalidTimeOption
			}
			if state.Since.After(*state.Until) {
				return ErrInvalidTimeOption
			}
		}
	}

	return nil
}

var (
	ErrInvalidQueryOption = errors.New("invalid query option")
)

func (state *State) checkQuery() error {
	if state.Query != "" {
		if state.Where != "" {
			return ErrInvalidQueryOption
		}
		if state.Limit != 0 {
			return ErrInvalidQueryOption
		}
	}

	return nil
}

func (state *State) checkFileFormat() error {
	var count int
	for _, format := range []bool{state.IsCSV, state.IsAlbLogs, state.IsCfLogs} {
		if format {
			count++
		}
		if count > 1 {
			return errors.Errorf("too many option: --csv, --alb-logs or --cf-logs")
		}
	}

	return nil
}
