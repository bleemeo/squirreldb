package main

import (
	"context"
	"fmt"

	"github.com/bleemeo/squirreldb/config"
	"github.com/bleemeo/squirreldb/core"
	"github.com/bleemeo/squirreldb/core/processsetup"
	"github.com/bleemeo/squirreldb/queryable/convert"
	"github.com/bleemeo/squirreldb/tests/common/testcases"
	"github.com/bleemeo/squirreldb/tests/common/testsetup"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

func main() {
	testsetup.SetTestEnvironment()

	if err := processsetup.RunWithSignalHandler(run); err != nil {
		log.Fatal().Msgf("Run failed: %s", err.Error())
	}
}

func run(ctx context.Context, shutdownCtx context.Context) error {
	cfg, warnings, err := testsetup.LoadConfigForTest()
	if err != nil {
		return err
	}

	if warnings != nil {
		return warnings
	}

	rwCases := testcases.GetReadWriteTestCases()
	for _, c := range rwCases {
		if err := doTest(ctx, shutdownCtx, cfg, c); err != nil {
			return err
		}
	}

	return nil
}

func doTest(ctx context.Context, shutdownCtx context.Context, cfg config.Config, c testcases.ReadWriteTestCase) error {
	log.Printf("Running test for %s", c.Name)

	squirreldb := core.New(
		core.Deps{
			MetricRegistry: prometheus.NewRegistry(),
			Logger:         log.Logger,
		},
		cfg,
		core.SquirrelDBOption{
			InternalTesting: core.InternalTestingOption{
				DropDataOnStartup: true, // the backend drops its data when it first starts
			},
		},
	)
	defer squirreldb.CloseAll(shutdownCtx)

	log.Printf("Dropping tables")

	if err := squirreldb.TestTools().DropTemporaryStore(ctx, false); err != nil {
		return fmt.Errorf("failed to drop keyspace: %w", err)
	}

	backend, err := squirreldb.Primary(ctx)
	if err != nil {
		return err
	}

	db, err := backend.TSDB(ctx)
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
