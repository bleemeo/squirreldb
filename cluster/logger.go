package cluster

import (
	"squirreldb/debug"
	"strings"
)

type filteringLogger struct {
}

func (l filteringLogger) Write(p []byte) (n int, err error) {
	str := strings.TrimSpace(string(p))

	switch {
	case strings.HasPrefix(str, "[ERR]"):
		str = strings.TrimPrefix(str, "[ERR]")
		logger.Print(str)
	case strings.HasPrefix(str, "[WARN]"):
		str = strings.TrimPrefix(str, "[WARN]")
		debug.Print(debug.Level1, logger, "%s", str)
	case strings.HasPrefix(str, "[INFO]"):
		str = strings.TrimPrefix(str, "[INFO]")
		debug.Print(debug.Level1, logger, "%s", str)
	case strings.HasPrefix(str, "[DEBUG]"):
		str = strings.TrimPrefix(str, "[DEBUG]")
		debug.Print(debug.Level2, logger, "%s", str)
	default:
		logger.Print(str)
	}

	return len(p), nil
}
