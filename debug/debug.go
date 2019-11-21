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

var CurrentLevel = 0

// Print displays a debug message according to its level
func Print(level int, logger *log.Logger, msg string, v ...interface{}) {
	if level <= CurrentLevel {
		levelString := strconv.Itoa(level)
		prefix := "[DEBUG-" + levelString + "]"

		logger.Printf(prefix+" "+msg, v...)
	}
}
