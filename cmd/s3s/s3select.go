package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/koluku/s3s"
	"golang.org/x/sync/errgroup"
)

type query struct {
	// Basic Query Info
	query   string
	where   string
	limit   int
	isCount bool
}

func (q *query) build() string {
	if q.query != "" {
		return q.query
	}
	if q.where == "" && q.limit == 0 && !q.isCount {
		return DEFAULT_QUERY
	}

	str := "SELECT"
	if q.isCount {
		str += " COUNT(*)"
	} else {
		str += " *"
	}
	str += " FROM S3Object s"
	if q.where != "" {
		str += " WHERE " + q.where
	}
	if q.limit != 0 {
		str += " LIMIT " + strconv.Itoa(q.limit)
	}
	return str
}

func execS3Select(ctx context.Context, app *s3s.App, ch chan s3s.Path, queryStr string, queryOption *s3s.S3SelectOption) error {
	resultReciever := make(chan s3s.S3SelectResult, DEFAULT_POOL_SIZE)
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := selectOneKey(egctx, app, ch, resultReciever, queryStr, queryOption); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		if err := writeResult(egctx, resultReciever); err != nil {
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func selectOneKey(ctx context.Context, app *s3s.App, reciever <-chan s3s.Path, sender chan<- s3s.S3SelectResult, queryStr string, queryOption *s3s.S3SelectOption) error {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(threadCount)
	for r := range reciever {
		bucket := r.Bucket
		key := r.Key
		eg.Go(func() error {
			if err := s3s.S3SelectWithChannel(egctx, app, bucket, key, queryStr, isCount, sender, queryOption); err != nil {
				return err
			}
			return nil
		})
	}

	err := eg.Wait()
	close(sender)
	if err != nil {
		return err
	}

	return nil
}

func writeResult(ctx context.Context, reciever <-chan s3s.S3SelectResult) error {
	var wtr = bufio.NewWriter(os.Stdout)
	var count int
	for r := range reciever {
		if isCount {
			count += r.Count
		} else {
			fmt.Fprint(wtr, r.Value)
		}
	}
	if isCount {
		fmt.Fprintln(wtr, count)
	}
	wtr.Flush()

	return nil
}
