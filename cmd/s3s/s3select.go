package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/koluku/s3s"
	"golang.org/x/sync/errgroup"
)

func execS3Select(ctx context.Context, app *s3s.App, ch chan s3s.Path) error {
	resultReciever := make(chan s3s.S3SelectResult, DEFAULT_POOL_SIZE)
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := selectOneKey(egctx, app, ch, resultReciever); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		return writeResult(egctx, resultReciever)
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func selectOneKey(ctx context.Context, app *s3s.App, reciever <-chan s3s.Path, sender chan<- s3s.S3SelectResult) error {
	eg, egctx := errgroup.WithContext(ctx)
	eg.SetLimit(threadCount)
	for r := range reciever {
		bucket := r.Bucket
		key := r.Key
		eg.Go(func() error {
			if err := s3s.S3SelectWithChannel(egctx, app, bucket, key, query, isCount, sender); err != nil {
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
