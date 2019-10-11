package config

var configFlags = []flag{
	{
		name:  "help",
		short: "h",
		value: false,
		usage: "Display help",
	},
	{
		name:  "prometheus.listen_address",
		short: "",
		value: "localhost:1234",
		usage: "Set the Prometheus listen address",
	},
	{
		name:  "cassandra.addresses",
		short: "",
		value: []string{"localhost:9042"},
		usage: "Set the Cassandra cluster addresses",
	},
	{
		name:   "test",
		short:  "",
		value:  false,
		usage:  "Display config file and flags variables and values (only for testing)",
		hidden: true,
	},
}
