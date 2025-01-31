// Copyright 2015-2025 Bleemeo
//
// bleemeo.com an infrastructure monitoring solution in the Cloud
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
)

// stringToMapHookFunc converts a string to map.
// It assumes the following format: "k1=v1,k2=v2".
// This is used to override map settings from environment variables.
func stringToMapHookFunc() mapstructure.DecodeHookFuncType {
	return func(source reflect.Type, target reflect.Type, data interface{}) (interface{}, error) {
		if source.Kind() != reflect.String || target.Kind() != reflect.Map {
			return data, nil
		}

		strMap, _ := data.(string)

		result := make(map[string]interface{})

		elementsList := strings.Split(strMap, ",")
		for i, element := range elementsList {
			values := strings.Split(element, "=")

			if i == len(elementsList)-1 && element == "" {
				return result, nil
			}

			if len(values) < 2 {
				err := fmt.Errorf("%w: '%s'", errWrongMapFormat, strMap)

				return make(map[string]interface{}), err
			}

			result[strings.TrimLeft(values[0], " ")] = strings.TrimRight(strings.Join(values[1:], "="), " ")
		}

		return result, nil
	}
}

// intToTimeDurationHookFunc convert int to seconds.
func intToTimeDurationHookFunc() mapstructure.DecodeHookFunc {
	return func(source reflect.Type, target reflect.Type, data interface{}) (interface{}, error) {
		if source.Kind() != reflect.Int || target != reflect.TypeOf(time.Duration(5)) {
			return data, nil
		}

		intData, _ := data.(int)

		return time.Duration(intData) * time.Second, nil
	}
}
