package remotestorage

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"context"
	"log"
	"net/http"
	"os"
	"squirreldb/retry"
	"squirreldb/types"
	"time"
)

const (
	metricsPattern = "/metrics"
	readPattern    = "/read"
	writePattern   = "/write"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type Options struct {
	ListenAddress string
	WithUUID      bool
}

type RemoteStorage struct {
	server *http.Server

	readMetrics  ReadMetrics
	writeMetrics WriteMetrics
}

// New creates a new RemoteStorage object
func New(options Options, indexer types.Indexer, reader types.MetricReader, writer types.MetricWriter) *RemoteStorage {
	router := http.NewServeMux()
	readMetrics := ReadMetrics{
		withUUID: options.WithUUID,
		indexer:  indexer,
		reader:   reader,
	}
	writeMetrics := WriteMetrics{
		indexer: indexer,
		writer:  writer,
	}

	router.Handle(metricsPattern, promhttp.Handler())
	router.HandleFunc(readPattern, readMetrics.ServeHTTP)
	router.HandleFunc(writePattern, writeMetrics.ServeHTTP)

	server := &http.Server{
		Addr:    options.ListenAddress,
		Handler: router,
	}

	remoteStorage := &RemoteStorage{
		server:       server,
		readMetrics:  readMetrics,
		writeMetrics: writeMetrics,
	}

	return remoteStorage
}

// Run starts all RemoteStorage services
func (p *RemoteStorage) Run(ctx context.Context) {
	p.runServer(ctx)
}

// Starts the server
// If a stop signal is received, the server will be stopped
// If 10 seconds after this signal is received, and the server is not stopped, it is forced to stop.
func (p *RemoteStorage) runServer(ctx context.Context) {
	go func() {
		retry.Print(func() error {
			err := p.server.ListenAndServe()

			if err == http.ErrServerClosed {
				return nil
			}

			return err
		}, retry.NewExponentialBackOff(30*time.Second), logger,
			"Error: Can't listen and serve the server",
			"Resolved: Listen and serve the server")
	}()

	logger.Printf("Server listening on %s", p.server.Addr)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Error: Can't stop the server (%v)", err)
	}

	logger.Println("Server stopped")
}
