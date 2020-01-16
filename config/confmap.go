package config

import (
	"errors"

	"github.com/knadh/koanf/maps"
)

// Confmap implements a raw map[string]interface{} provider.
// Highly inspired by koanf confmap.Provider but without the deep-copy that convert
// all number to float64.
type Confmap struct {
	mp map[string]interface{}
}

// ConfMapProvider returns a confmap Provider that takes a flat map[string]interface{}.
// If a delim is provided, it indicates that the keys are flat and the map needs
// to be unflatted by delim.
func ConfMapProvider(mp map[string]interface{}, delim string) *Confmap {
	cp := make(map[string]interface{}, len(mp))

	for k, v := range mp {
		cp[k] = v
	}

	maps.IntfaceKeysToStrings(cp)

	if delim != "" {
		cp = maps.Unflatten(cp, delim)
	}

	return &Confmap{mp: cp}
}

// ReadBytes is not supported by the env provider.
func (e *Confmap) ReadBytes() ([]byte, error) {
	return nil, errors.New("confmap provider does not support this method")
}

// Read returns the loaded map[string]interface{}.
func (e *Confmap) Read() (map[string]interface{}, error) {
	return e.mp, nil
}

// Watch is not supported.
func (e *Confmap) Watch(cb func(event interface{}, err error)) error {
	return errors.New("confmap provider does not support this method")
}
