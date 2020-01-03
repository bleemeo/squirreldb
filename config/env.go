package config

import (
	"strings"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/maps"
	"github.com/knadh/koanf/providers/env"
)

// An Koanf.Provider for environment that support string list.
// list items are separated by ","
// To decide is something is a list of not, we use defaults
type listEnvProvider struct {
	koanf.Provider
}

func newEnvProvider() koanf.Provider {
	ep := listEnvProvider{}
	ep.Provider = env.Provider(envPrefix, delimiter, func(s string) string {
		s = strings.TrimPrefix(s, envPrefix)

		key, exists := envToKey[s]

		if !exists {
			logger.Printf("Warning: '%s' environment variable doesn't exist.", s)
		}

		return key
	})

	return ep
}

func (ep listEnvProvider) Read() (map[string]interface{}, error) {
	mp, err := ep.Provider.Read()
	if err != nil {
		return mp, err
	}

	flat, _ := maps.Flatten(mp, nil, delimiter)
	for k, v := range flat {
		defaultValue, ok := defaults[k]
		if ok {
			if _, ok := defaultValue.([]string); ok {
				vStr, ok := v.(string)
				if !ok {
					continue
				}

				flat[k] = strings.Split(vStr, ",")
			}
		}
	}

	mp = maps.Unflatten(flat, ".")

	return mp, err
}
