package config

var flags = []flag{
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
		usage:  "",
		hidden: true,
	},
}
