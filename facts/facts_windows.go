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
	"errors"
	"fmt"
	"log"
	"os"

	"golang.org/x/sys/windows/registry"
)

//nolint: gochecknoglobals
var logger = log.New(os.Stdout, "[index] ", log.LstdFlags)

var errNoResults = errors.New("the WMI request returned 0 result")

const unsupportedVersion = "Unsupported Version"

func matchClientVersion(major, minor uint64) string {
	if major == 10 {
		return "10"
	}

	switch minor {
	case 0:
		return "Vista"
	case 1:
		return "7"
	case 2:
		return "8"
	case 3:
		return "8.1"
	default:
		return unsupportedVersion
	}
}

func matchServerVersion(major, minor uint64) string {
	if major == 10 {
		return "Server 2016 or higher"
	}

	switch minor {
	case 0:
		return "Server 2008"
	case 1:
		return "Server 2008 R2"
	case 2:
		return "Server 2012"
	case 3:
		return "Server 2012 R2"
	default:
		return unsupportedVersion
	}
}

func getWindowsVersionName(major uint64, minor uint64, isServer bool, servicePack string) string {
	if major != 6 && major != 10 {
		return unsupportedVersion
	}

	var res string

	var version string

	if isServer {
		version = matchServerVersion(major, minor)
	} else {
		version = matchClientVersion(major, minor)
	}

	if servicePack != "" {
		res = fmt.Sprintf("Windows %s SP %s", version, servicePack)
	} else {
		res = fmt.Sprintf("Windows %s", version)
	}

	return res
}

func platformFacts() map[string]string {
	facts := make(map[string]string)

	facts["os_name"] = "Windows"

	reg, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\Microsoft\Windows NT\CurrentVersion`, registry.QUERY_VALUE)
	if err != nil {
		logger.Println("Couldn't open the windows registry, some facts may not be exposed")
	} else {
		defer reg.Close()

		major, _, err1 := reg.GetIntegerValue("CurrentMajorVersionNumber")
		minor, _, err2 := reg.GetIntegerValue("CurrentMinorVersionNumber")
		servicePack, _, _ := reg.GetStringValue("CSDVersion")
		installationType, _, _ := reg.GetStringValue("InstallationType")

		isServer := true
		if installationType == "Client" {
			isServer = false
		}

		if err1 == nil && err2 == nil {
			facts["os_version"] = getWindowsVersionName(major, minor, isServer, servicePack)
		}

	}

	return facts
}
