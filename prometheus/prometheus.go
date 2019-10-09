package prometheus

import (
	"context"
	"github.com/cenkalti/backoff"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	logger  = log.New(os.Stdout, "[prometheus] ", log.LstdFlags)
	backOff = backoff.ExponentialBackOff{
		InitialInterval:     backoff.DefaultInitialInterval,
		RandomizationFactor: 0.5,
		Multiplier:          2,
		MaxInterval:         30 * time.Second,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
)

type Prometheus struct {
	readPoints  ReadPoints
	writePoints WritePoints
}

func NewPrometheus(readPoints ReadPoints, writePoints WritePoints) *Prometheus {
	return &Prometheus{
		readPoints:  readPoints,
		writePoints: writePoints,
	}
}

func (p *Prometheus) RunServer(ctx context.Context, wg *sync.WaitGroup) {
	router := http.NewServeMux()
	server := http.Server{
		Addr:    "localhost:1234", // TODO: Prometheus address from config
		Handler: router,
	}

	router.HandleFunc("/read", p.readPoints.ServeHTTP)
	router.HandleFunc("/write", p.writePoints.ServeHTTP)

	go func() {
		_ = backoff.Retry(func() error {
			err := server.ListenAndServe()

			if err != http.ErrServerClosed {
				logger.Println("RunServer: Can't listen and serve the server (", err, ")")
				return err
			}

			return nil
		}, &backOff)
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("RunServer: Error while stopping server (%v)"+"\n", err)
	}

	logger.Println("RunServer: Stopped")
	wg.Done()
}
