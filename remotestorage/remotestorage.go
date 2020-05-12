package remotestorage

import (
	"fmt"

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
	flushPattern              = "/flush"
	preAggregatePattern       = "/debug_preaggregate"
	httpServerShutdownTimeout = 10 * time.Second
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[remotestorage] ", log.LstdFlags)

type Options struct {
	ListenAddress        string
	FlushCallback        func() error
	PreAggregateCallback func(from, to time.Time) error
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
	router.HandleFunc(flushPattern, func(w http.ResponseWriter, req *http.Request) {
		if options.FlushCallback != nil {
			err := options.FlushCallback()
			if err != nil {
				logger.Printf("flush failed: %v", err)
				http.Error(w, "Flush failed", http.StatusInternalServerError)
				return
			}
		}
		fmt.Fprintln(w, "Flush points (of this SquirrelDB instance) from temporary store to TSDB done")
	})
	router.HandleFunc(preAggregatePattern, func(w http.ResponseWriter, req *http.Request) {
		fromRaw := req.URL.Query().Get("from")
		toRaw := req.URL.Query().Get("to")

		from, err := time.Parse(time.RFC3339, fromRaw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		to, err := time.Parse(time.RFC3339, toRaw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		start := time.Now()
		if options.PreAggregateCallback != nil {
			err := options.PreAggregateCallback(from, to)
			if err != nil {
				logger.Printf("pre-aggregation failed: %v", err)
				http.Error(w, "pre-aggregation failed", http.StatusInternalServerError)
				return
			}
		}
		fmt.Fprintf(w, "Pre-aggregation terminated in %v\n", time.Since(start))
	})

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
