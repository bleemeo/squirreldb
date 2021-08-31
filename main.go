package main

import (
	"log"
	"math/rand"
	"os"
	"squirreldb/daemon"
	"squirreldb/debug"
	"time"
)

//nolint:gochecknoglobals
var logger = log.New(os.Stdout, "[main] ", log.LstdFlags)

// variable set by GoReleaser
//nolint:gochecknoglobals
var (
	version string
	commit  string
	date    string
)

func main() {
	rand.Seed(time.Now().UnixNano())

	daemon.Version = version
	daemon.Commit = commit
	daemon.Date = date

	cfg, err := daemon.Config()
	if err != nil {
		logger.Fatal(err)
	}

	squirreldb := &daemon.SquirrelDB{
		Config: cfg,
	}

	logger.Printf("Starting SquirrelDB %s (commit %s)", version, commit)

	err = daemon.RunWithSignalHandler(squirreldb.Run)
	if err != nil {
		logger.Printf("Failed to run SquirrelDB: %v", err)
	}

	debug.Print(1, logger, "SquirrelDB is stopped")
}
