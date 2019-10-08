package prometheus

import (
	"context"
	"github.com/cenkalti/backoff"
	"log"
	"net/http"
	"os"
	"squirreldb/config"
	"squirreldb/types"
	"sync"
	"time"
)

type Prometheus struct {
	readPoints  ReadPoints
	writePoints WritePoints
}

var (
	logger = log.New(os.Stdout, "[prometheus] ", log.LstdFlags)
)

// NewPrometheus creates a new Prometheus object
func NewPrometheus(reader types.MetricReader, writer types.MetricWriter) *Prometheus {
	return &Prometheus{
		readPoints:  ReadPoints{reader: reader},
		writePoints: WritePoints{writer: writer},
	}
}

// RunServer run the server to receive write and read requests.
// If the context receives a stop signal, the server is stopped
func (p *Prometheus) RunServer(ctx context.Context, wg *sync.WaitGroup) {
	router := http.NewServeMux()
	server := http.Server{
		Addr:    config.C.String("prometheus.address"),
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
		}, &backoff.ExponentialBackOff{
			InitialInterval:     backoff.DefaultInitialInterval,
			RandomizationFactor: 0.5,
			Multiplier:          2,
			MaxInterval:         30 * time.Second,
			MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
			Clock:               backoff.SystemClock,
		})
	}()

	// Wait to receive a stop signal
	<-ctx.Done()

	// Stop the server
	// If it is not stopped within 10 seconds, the stop is forced
	subCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(subCtx); err != nil {
		logger.Printf("RunServer: Error while stopping server (%v)"+"\n", err)
	}

	logger.Println("RunServer: Stopped")
	wg.Done()
}
