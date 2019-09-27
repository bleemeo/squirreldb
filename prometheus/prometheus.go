package prometheus

import (
	"context"
	"log"
	"net/http"
	"os"
	"squirreldb/config"
	"squirreldb/retry"
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

func NewPrometheus(reader types.MetricReader, writer types.MetricWriter) *Prometheus {
	return &Prometheus{
		readPoints:  ReadPoints{reader: reader},
		writePoints: WritePoints{writer: writer},
	}
}

func (p *Prometheus) RunServer(ctx context.Context, wg *sync.WaitGroup) {
	router := http.NewServeMux()
	server := http.Server{
		Addr:    config.PrometheusAddress,
		Handler: router,
	}

	router.HandleFunc("/read", p.readPoints.ServeHTTP)
	router.HandleFunc("/write", p.writePoints.ServeHTTP)

	go retry.Endlessly(config.PrometheusRetryDelay, func() error {
		err := server.ListenAndServe()

		if err != http.ErrServerClosed {
			logger.Printf("RunServer: Can't listen and serve (%v)"+"\n", err)

			return err
		}

		return nil
	}, logger)

	<-ctx.Done()

	logger.Println("RunServer: Stopping...")

	subCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(subCtx); err != nil {
		logger.Printf("Error while stopping server (%v)"+"\n", err)
	}

	logger.Println("RunServer: Stopped")
	wg.Done()
}
