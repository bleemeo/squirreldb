package config

//nolint: gochecknoglobals
var flags = []flag{
	{
		name:  "help",
		short: "h",
		value: false,
		usage: "Display help",
	},
	{
		name:  "cassandra.addresses",
		short: "",
		value: []string{"localhost:9042"},
		usage: "Set the Cassandra cluster addresses",
	},
	{
		name:  "redis.address",
		short: "",
		value: "localhost:6379",
		usage: "Set the Redis address",
	},
	{
		name:  "remote_storage.listen_address",
		short: "",
		value: "localhost:1234",
		usage: "Set the remote storage listen address",
	},
	{
		name:  "bypass-validate",
		short: "",
		value: false,
		usage: "Bypass validate",
	},
}

type flag struct {
	name   string
	short  string
	value  interface{}
	usage  string
	hidden bool
}
