package dummy

import (
	"fmt"
	"strconv"
	"sync"
)

// States is a in-memory single-node "states", that is a map :)
// Only useful for tests.
type States struct {
	mutex  sync.Mutex
	values map[string]string
}

func (s *States) Read(name string, value interface{}) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// We mimick behavior of cassandra/states
	valueString, ok := s.values[name]
	if !ok {
		return false, nil
	}

	switch v := value.(type) {
	case *float64:
		valueFloat64, _ := strconv.ParseFloat(valueString, 64)
		*v = valueFloat64
	case *int:
		valueInt, _ := strconv.Atoi(valueString)
		*v = valueInt
	case *int64:
		valueInt64, _ := strconv.ParseInt(valueString, 10, 64)
		*v = valueInt64
	case *string:
		*v = valueString
	default:
		return false, fmt.Errorf("unknown type")
	}

	return true, nil
}

func (s *States) Write(name string, value interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.values == nil {
		s.values = make(map[string]string)
	}

	valueString := fmt.Sprint(value)
	s.values[name] = valueString

	return nil
}
