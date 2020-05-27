package olric

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"squirreldb/dummy"
	"squirreldb/types"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[olric] ", log.LstdFlags)

type Wal struct {
	dummy.DiscardTSDB

	instanceID int
	db         *olric.Olric
	walMap     *olric.DMap
	errChan    chan error
}

func (w *Wal) Init() error {
	w.instanceID = rand.Int()
	ready := make(chan interface{})
	cfg := config.New("local")

	cfg.Started = func() {
		close(ready)
	}

	db, err := olric.New(cfg)
	if err != nil {
		return err
	}

	w.db = db
	w.errChan = make(chan error, 1)

	go func() {
		err := db.Start()
		w.errChan <- err
	}()

	select {
	case <-ready:
	case err = <-w.errChan:
	}

	if err != nil {
		w.Close()

		return err
	}

	w.walMap, err = w.db.NewDMap("wal-map")
	if err != nil {
		w.Close()

		return err
	}

	return nil
}

func (w *Wal) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := w.db.Shutdown(ctx)
	if err != nil {
		logger.Printf("Error during shutdown: %v", err)
	}
}

func (w *Wal) Write(metrics []types.MetricData) error {
	key := fmt.Sprintf("pid-%d-rnd-%d-ts-%d", os.Getgid(), w.instanceID, time.Now().Unix())

	w.walMap.Put(key, metrics)

	return nil
}
