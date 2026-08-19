// Copyright 2015-2026 Bleemeo
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

package testcases

import "github.com/prometheus/prometheus/model/labels"

const (
	LabelName    = "__name__"
	AccountLabel = "__label_for_tenant__"
	Tenant1      = "1234"
	Tenant2      = "5678"
	MetricName1  = "disk_used"
	MetricName2  = "disk_free"
	DiskItem     = "mountpath"
	DiskItem2    = "fstype"
	DiskMount1   = "/home"
	DiskMount2   = "/srv"
	DiskFS1      = "ext4"
	DefaultTTL   = int64(86400)
)

type Constant struct {
	LabelsMetric1 labels.Labels
	LabelsMetric2 labels.Labels
	LabelsMetric3 labels.Labels
}

// GetConstant return stuff that should be const but can't be because they are function result.
func GetConstant() Constant {
	labelsMetric1 := labels.FromMap(map[string]string{
		AccountLabel: Tenant1,
		LabelName:    MetricName1,
		DiskItem:     DiskMount1,
	})
	labelsMetric2 := labels.FromMap(map[string]string{
		AccountLabel: Tenant2,
		LabelName:    MetricName1,
		DiskItem:     DiskMount2,
	})
	labelsMetric3 := labels.FromMap(map[string]string{
		AccountLabel: Tenant2,
		LabelName:    MetricName2,
		DiskItem:     DiskMount2,
		DiskItem2:    DiskFS1,
	})

	return Constant{
		LabelsMetric1: labelsMetric1,
		LabelsMetric2: labelsMetric2,
		LabelsMetric3: labelsMetric3,
	}
}
