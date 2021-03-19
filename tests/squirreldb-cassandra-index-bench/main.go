package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"squirreldb/cassandra/index"
	"squirreldb/config"
	"squirreldb/daemon"
	"squirreldb/dummy"
	"squirreldb/types"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

//nolint: gochecknoglobals
var (
	runExpiration     = flag.Bool("bench.expiration", false, "Run the expiration (should delete 1/2 of metrics")
	expiredFaction    = flag.Int("expired-fraction", 2, "one part over N of the metric will be expired")
	defaultTimeToLive = flag.Duration("index.ttl", 365*24*time.Hour, "Default time to live")
	seed              = flag.Int64("bench.seed", 42, "Seed used in random generator")
	sortInsert        = flag.Bool("bench.insert-sorted", false, "Keep label sorted at insertion time (Prometheus do it)")
	queryCount        = flag.Int("bench.query", 100, "Number of query to run")
	skipWrite         = flag.Bool("bench.skip-write", false, "Do not insert into index for benchmark. Useful with -no-drop and a previous run that filled index")
	skipValid         = flag.Bool("skip-validation", false, "Do not run the validation and only run benchmark")
	onlyQuery         = flag.String("bench.only-query", "", "Only run the query that exactly match the name")
	queryMaxTime      = flag.Duration("bench.max-time", 5*time.Second, "Maxium time for one query time")
	shardSize         = flag.Int("bench.shard-size", 100, "How many metrics to add in one shard (a shard is a label with the same value. Think tenant)")
	shardStart        = flag.Int("bench.shard-start", 1, "Start at shard number N")
	shardEnd          = flag.Int("bench.shard-end", 5, "End at shard number N (included)")
	insertBatchSize   = flag.Int("bench.batch-size", 1000, "Number of metrics to lookup at once")
	noDropTables      = flag.Bool("no-drop", false, "Don't drop tables before (in such case, you should not change shardSize and seed)")
	workerThreads     = flag.Int("bench.worker-max-threads", 1, "Number of concurrent threads inserting data (1 == not threaded)")
	workerProcesses   = flag.Int("bench.worker-processes", 1, "Number of concurrent index (equivalent to process) inserting data")
	workerClients     = flag.Int("bench.worker-client", 1, "Number of concurrent client inserting data")
	fairLB            = flag.Bool("force-fair-lb", false, "Force fair load-balancing even if worker is busy")
	verify            = flag.Bool("verify", false, "Run the index verification process")
	cpuprofile        = flag.String("cpuprofile", "", "write cpu profile to file")
)

type Factory struct {
	l       sync.Mutex
	cfg     *config.Config
	cluster types.Cluster
}

func (f *Factory) makeIndex(ctx context.Context) types.Index {
	f.l.Lock()

	if f.cluster == nil {
		f.cluster = &dummy.LocalCluster{}
	}

	f.l.Unlock()

	squirreldb := &daemon.SquirrelDB{
		Config:          f.cfg,
		ExistingCluster: f.cluster,
	}

	idx, err := squirreldb.Index(ctx, false)
	if err != nil {
		log.Fatalf("Unable to create index: %v", err)
	}

	return idx
}

func main() {
	daemon.SetTestEnvironment()

	cfg, err := daemon.Config()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if !*runExpiration && *verify {
		log.Println("Force running expiration because index verify is enabled")

		*runExpiration = true
	}

	factory := &Factory{
		cfg: cfg,
	}

	if !*noDropTables {
		log.Printf("Droping tables")

		squirreldb := &daemon.SquirrelDB{
			Config: cfg,
		}

		err := squirreldb.DropCassandraData(ctx, false)
		if err != nil {
			log.Fatalf("failed to drop keyspace: %v", err)
		}
	}

	rand.Seed(*seed)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}

		defer pprof.StopCPUProfile()
	}

	if !*skipValid {
		cassandraIndex := factory.makeIndex(ctx)

		log.Printf("Start validating test")
		test(ctx, cassandraIndex)
		log.Printf("Re-run validating test")
		test(ctx, cassandraIndex)
		log.Printf("Re-run validating test on fresh index")
		test(ctx, factory.makeIndex(ctx))
	}

	rnd := rand.New(rand.NewSource(*seed)) //nolint: gosec
	bench(ctx, factory.makeIndex, rnd)

	verifyHadIssue := false

	if *verify {
		var err error

		idx := factory.makeIndex(ctx)
		cassandraIndex, ok := idx.(*index.CassandraIndex)

		if !ok {
			verifyHadIssue = true

			log.Println("Can not verify, index isn't a CassandraIndex")
		} else {
			verifyHadIssue, err = cassandraIndex.Verify(context.Background(), os.Stderr, false, false)

			if err != nil {
				log.Println(err)
			}
		}
	}

	result, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range result {
		_, _ = expfmt.MetricFamilyToText(os.Stdout, mf)
	}

	if verifyHadIssue {
		log.Println("Index verify had issue, see above")
	}
}
