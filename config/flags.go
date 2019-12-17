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
		usage: "Set the Cassandra cluster addresses",
	},
	{
		name:  "redis.address",
		short: "",
		usage: "Set the Redis address",
	},
	{
		name:  "remote_storage.listen_address",
		short: "",
		usage: "Set the remote storage listen address",
	},
	{
		name:  "ignore-config",
		short: "",
		value: false,
		usage: "Ignore the old configuration and use the current configuration",
	},
	{
		name:  "overwrite-config",
		short: "",
		value: false,
		usage: "Overwrite the old configuration with the current configuration",
	},
}

type flag struct {
	name   string
	short  string
	value  interface{}
	usage  string
	hidden bool
}
