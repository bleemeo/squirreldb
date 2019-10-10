package config

const (
	BatchCheckerInterval = 60
)

const (
	CassandraKeyspace            = "squirreldb"
	CassandraDataTable           = "data"
	CassandraAggregatedDataTable = "data_aggregated"
)

var configFileExtensions = []string{".conf", ".yaml", ".yml"}

const (
	configFolderRoot = "./"
	configEnvPrefix  = "SQUIRRELDB_"
	configDelimiter  = "."
)

const (
	LabelSpecialPrefix = "__bleemeo_"
)

const (
	StoreExpiratorInterval = 60
	StoreTimeToLiveOffset  = 150
)
