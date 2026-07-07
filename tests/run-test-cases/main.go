package main

import (
	"context"
	"fmt"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/daemon"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/tests/testcases"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

func main() {
	daemon.SetTestEnvironment()

	if err := daemon.RunWithSignalHandler(run); err != nil {
		log.Fatal().Msgf("Run failed: %s", err.Error())
	}
}

func run(ctx context.Context) error {
	cfg, warnings, err := daemon.Config()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	rwCases := testcases.GetReadWriteTestCases()
	for _, c := range rwCases {
		if err := doTest(ctx, cfg, c); err != nil {
			return err
		}
	}

	return nil
}

func doTest(ctx context.Context, cfg config.Config, c testcases.ReadWriteTestCase) error {
	log.Printf("Running test for %s", c.Name)

	squirreldb := &daemon.SquirrelDB{
		Config:            cfg,
		MetricRegistry:    prometheus.NewRegistry(),
		Logger:            log.Logger,
		DropDataOnStartup: true, // the backend drops its data when it first starts
	}
	defer squirreldb.Stop()

	log.Printf("Dropping tables")

	if err := squirreldb.DropTemporaryStore(ctx, false); err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	db, err := squirreldb.TSDB(ctx)
	if err != nil {
		return err
	}

	defer db.Close(ctx)

	store := convert.NewTSDBWithFixedOption(
		db,
		types.QuerierOptions{},
		types.AppenderOptions{},
	)
	if err := c.ExecCase(ctx, store); err != nil {
		return fmt.Errorf("case %s: %w", c.Name, err)
	}

	return nil
}
