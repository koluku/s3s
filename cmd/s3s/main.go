package main

import (
	"context"
	"log"
	"os"
	"os/signal"
)

const (
	DEFAULT_QUERY            = "SELECT * FROM S3Object s"
	DEFAULT_THREAD_COUNT     = 150
	DEFAULT_MAX_RETRIES      = 20
	DEFAULT_FIELD_DELIMITER  = ","
	DEFAULT_RECORD_DELIMITER = "\n"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	err := Command.RunContext(ctx, os.Args)
	if err != nil {
		if GlobalOption.Debug {
			log.Fatalf("%+v\n", err)
		} else {
			log.Fatal(err)
		}
	}
}
