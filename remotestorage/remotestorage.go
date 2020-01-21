package remotestorage

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"context"
	"log"
	"net/http"
	"os"
	"squirreldb/debug"
	"squirreldb/types"
	"time"
)

const (
	metricsPattern            = "/metrics"
	readPattern               = "/read"
	writePattern              = "/write"
	httpServerShutdownTimeout = 10 * time.Second
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type Options struct {
	ListenAddress string
}

type RemoteStorage struct {
	server *http.Server

	readMetrics  ReadMetrics
	writeMetrics WriteMetrics
}

// New creates a new RemoteStorage object
func New(options Options, index types.Index, reader types.MetricReader, writer types.MetricWriter) *RemoteStorage {
	router := http.NewServeMux()
	readMetrics := ReadMetrics{
		index:  index,
		reader: reader,
	}
	writeMetrics := WriteMetrics{
		index:    index,
		writer:   writer,
		reqCtxCh: make(chan *requestContext, 4),
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
func (r *RemoteStorage) Run(ctx context.Context) {
	r.runServer(ctx)
}

// Starts the server
// If a stop signal is received, the server will be stopped
// If 10 seconds after this signal is received, and the server is not stopped, it is forced to stop.
func (r *RemoteStorage) runServer(ctx context.Context) {
	serverStopped := make(chan interface{})

	go func() {
		err := r.server.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			logger.Printf("Failed to listen on %s", r.server.Addr)
		}

		close(serverStopped)
	}()

	logger.Printf("Server listening on %s", r.server.Addr)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), httpServerShutdownTimeout)
		defer cancel()

		if err := r.server.Shutdown(shutdownCtx); err != nil {
			logger.Printf("Error: Can't stop the server (%v)", err)
		}

		<-serverStopped
	case <-serverStopped:
	}

	debug.Print(2, logger, "Server stopped")
}
