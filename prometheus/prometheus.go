package prometheus

import (
	"context"
	"github.com/cenkalti/backoff"
	"log"
	"net/http"
	"os"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

var (
	logger = log.New(os.Stdout, "[prometheus] ", log.LstdFlags)
)

type Prometheus struct {
	readPoints  ReadPoints
	writePoints WritePoints
}

// New creates a new Prometheus object
func New(matcher types.MetricIndexer, reader types.MetricReader, writer types.MetricWriter) *Prometheus {
	return &Prometheus{
		readPoints: ReadPoints{
			indexer: matcher,
			reader:  reader,
		},
		writePoints: WritePoints{
			indexer: matcher,
			writer:  writer,
		},
	}
}

// Run run the server to receive write and read requests
// If the context receives a stop signal, the server is stopped
func (p *Prometheus) Run(ctx context.Context, listenAddress string) {
	router := http.NewServeMux()
	server := http.Server{
		Addr:    listenAddress,
		Handler: router,
	}

	router.HandleFunc("/read", p.readPoints.ServeHTTP)
	router.HandleFunc("/write", p.writePoints.ServeHTTP)

	go func() {
		_ = backoff.Retry(func() error {
			err := server.ListenAndServe()

			if err != http.ErrServerClosed {
				logger.Println("Run: Can't listen and serve the server (", err, ")")
				return err
			}

			return nil
		}, retry.NewBackOff(30*time.Second))
	}()

	// Wait to receive a stop signal
	<-ctx.Done()

	// Stop the server
	// If it is not stopped within 10 seconds, the stop is forced
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Println("Run: Error while stopping server (", err, ")")
	}

	logger.Println("Run: Stopped")
}
