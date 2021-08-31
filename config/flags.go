package config

//nolint:gochecknoglobals
var flags = []flag{
	{
		name:  "help",
		short: "h",
		value: false,
		usage: "Display help",
	},
	{
		name:  "version",
		short: "v",
		value: false,
		usage: "Show version and exit",
	},
	{
		name:  "build-info",
		short: "",
		value: false,
		usage: "Show build-info and exit",
	},
	{
		name:  "cassandra.addresses",
		short: "",
		value: defaults["cassandra.addresses"],
		usage: "Set the Cassandra cluster addresses",
	},
	{
		name:  "redis.address",
		short: "",
		value: defaults["redis.address"],
		usage: "Set the Redis address",
	},
	{
		name:  "remote_storage.listen_address",
		short: "",
		value: defaults["remote_storage.listen_address"],
		usage: "Set the remote storage listen address",
	},
	{
		name:  "overwite-previous-config",
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
