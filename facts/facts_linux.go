// Copyright 2015-2019 Bleemeo
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

package facts

import (
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func decodeOsRelease(data string) (map[string]string, error) {
	result := make(map[string]string)

	lines := strings.Split(data, "\n")
	for _, line := range lines {
		if line == "" || !strings.Contains(line, "=") {
			continue
		}

		t := strings.SplitN(line, "=", 2)
		key := t[0]

		if t[1] == "" {
			continue
		}

		if t[1][0] == '"' {
			value, err := strconv.Unquote(t[1])
			if err != nil {
				return nil, err
			}

			result[key] = value
		} else {
			result[key] = t[1]
		}
	}

	return result, nil
}

func platformFacts() map[string]string {
	facts := make(map[string]string)

	osReleasePath := filepath.Join("/", "etc/os-release")
	if osReleaseData, err := ioutil.ReadFile(osReleasePath); err != nil {
		logger.Printf("unable to read os-release file: %v", err)
	} else {
		osRelease, err := decodeOsRelease(string(osReleaseData))
		if err != nil {
			logger.Printf("os-release file is invalid: %v", err)
		}

		facts["os_name"] = osRelease["NAME"]
		facts["os_version"] = osRelease["VERSION_ID"]
	}

	var utsName unix.Utsname

	err := unix.Uname(&utsName)
	if err == nil {
		l := strings.SplitN(facts["kernel_release"], "-", 2)
		facts["kernel_version"] = l[0]
	}

	return facts
}
