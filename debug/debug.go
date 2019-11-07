package debug

import (
	"log"
	"strconv"
)

const (
	Level0 = 0
	Level1 = 1
	Level2 = 2
)

var Level = 0

// Print displays information according to the debug level
func Print(level int, logger *log.Logger, msg string, v ...interface{}) {
	if level >= Level {
		levelString := strconv.Itoa(level)
		prefix := "[DEBUG-" + levelString + "]"

		logger.Printf(prefix+" "+msg, v...)
	}
}
