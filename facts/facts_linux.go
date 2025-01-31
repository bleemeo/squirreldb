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

package facts

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
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

func bytesToString(buffer []byte) string {
	n := bytes.IndexByte(buffer, 0)

	return string(buffer[:n])
}

func platformFacts() map[string]string {
	facts := make(map[string]string)

	osReleasePath := filepath.Join("/", "etc/os-release")
	if osReleaseData, err := os.ReadFile(osReleasePath); err != nil {
		log.Debug().Err(err).Msg("Unable to read os-release file")
	} else {
		osRelease, err := decodeOsRelease(string(osReleaseData))
		if err != nil {
			log.Err(err).Msg("os-release file is invalid")
		}

		facts["os_name"] = osRelease["NAME"]
		facts["os_version"] = osRelease["VERSION_ID"]
	}

	var utsName unix.Utsname

	if err := unix.Uname(&utsName); err == nil {
		l := strings.SplitN(bytesToString(utsName.Release[:]), "-", 2)
		facts["kernel_version"] = l[0]
	}

	return facts
}
