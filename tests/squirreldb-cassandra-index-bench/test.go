package main

import (
	"log"
	"reflect"
	"squirreldb/types"
	"strconv"

	"github.com/prometheus/prometheus/pkg/labels"
)

func test(cassandraIndex types.Index) { //nolint: gocognit
	metrics := []map[string]string{
		{}, // index 0 is skipped to distinguish "not found" from 0
		{ // index 1
			"__name__": "up",
			"job":      "prometheus",
			"instance": "localhost:9090",
		},
		{ // index 2
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "localhost:9100",
		},
		{ // index 3
			"__name__": "up",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
		},
		{ // index 4
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "idle",
		},
		{ // index 5
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "0",
			"mode":     "user",
		},
		{ // index 6
			"__name__": "node_cpu_seconds_total",
			"job":      "node_exporter",
			"instance": "remotehost:9100",
			"cpu":      "1",
			"mode":     "user",
		},
		{ // index 7
			"__name__":   "node_filesystem_avail_bytes",
			"job":        "node_exporter",
			"instance":   "localhost:9100",
			"device":     "/dev/mapper/vg0-root",
			"fstype":     "ext4",
			"mountpoint": "/",
		},
		{ // index 8
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "localhost:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "devel",
		},
		{ // index 9
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
		},
		{ // index 10
			"__name__":    "node_filesystem_avail_bytes",
			"job":         "node_exporter",
			"instance":    "remote:9100",
			"device":      "/dev/mapper/vg0-data",
			"fstype":      "ext4",
			"mountpoint":  "/srv/data",
			"environment": "production",
			"userID":      "42",
		},
	}

	tests := []struct {
		Name            string
		Matchers        []*labels.Matcher
		MatchingMetrics []int
	}{
		{
			Name: "eq",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "up",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "eq-eq",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  labels.MatchEqual,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "eq-neq",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  labels.MatchNotEqual,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "eq-nolabel",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchEqual,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "eq-label",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchNotEqual,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "u.",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "re-re",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_cpu_.*",
				},
				{
					Type:  labels.MatchRegexp,
					Name:  "mode",
					Value: "^u.*",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "re-nre",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_(cpu|disk)_seconds_total",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "mode",
					Value: "u\\wer",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "re-re_nolabel",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchRegexp,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "re-re_label",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re-re*",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  labels.MatchRegexp,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{7, 8, 9, 10},
		},
		{
			Name: "re-nre*",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchRegexp,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{},
		},
		{
			Name: "eq-nre_empty_and_devel",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "environment",
					Value: "(|devel)",
				},
			},
			MatchingMetrics: []int{9, 10},
		},
		{
			Name: "eq-nre-eq same label",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchNotRegexp,
					Name:  "environment",
					Value: "^$",
				},
				{
					Type:  labels.MatchEqual,
					Name:  "environment",
					Value: "devel",
				},
			},
			MatchingMetrics: []int{8},
		},
		{
			Name: "eq-eq-no_label",
			Matchers: []*labels.Matcher{
				{
					Type:  labels.MatchEqual,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  labels.MatchEqual,
					Name:  "environment",
					Value: "production",
				},
				{
					Type:  labels.MatchEqual,
					Name:  "userID",
					Value: "",
				},
			},
			MatchingMetrics: []int{9},
		},
	}

	metricsIDs := make([]types.MetricID, len(metrics))
	ID2metrics := make(map[types.MetricID]int, len(metrics))

	for i, labelsMap := range metrics {
		if i == 0 {
			continue
		}

		ids, ttls, err := cassandraIndex.LookupIDs([]labels.Labels{labels.FromMap(labelsMap)})
		if err != nil {
			log.Fatalf("LookupIDs(%v) failed: %v", labelsMap, err)
		}

		if ttls[0] != int64(defaultTimeToLive.Seconds()) {
			log.Fatalf("TTL = %d, want default TTL (%d)", ttls[0], *defaultTimeToLive)
		}

		metricsIDs[i] = ids[0]
		ID2metrics[ids[0]] = i
	}

	for _, tt := range tests {
		wantIDToIndex := make(map[types.MetricID]int, len(tt.MatchingMetrics))

		for _, m := range tt.MatchingMetrics {
			wantIDToIndex[metricsIDs[m]] = m
		}

		ids, err := cassandraIndex.Search(tt.Matchers)

		if err != nil {
			log.Fatalf("Search(%s) failed: %v", tt.Name, err)
		}

		gotIDToIndex := make(map[types.MetricID]int, len(ids))

		for _, id := range ids {
			gotIDToIndex[id] = ID2metrics[id]
		}

		if !reflect.DeepEqual(wantIDToIndex, gotIDToIndex) {
			log.Fatalf("Search(%s) = %v, want %v", tt.Name, gotIDToIndex, wantIDToIndex)
		}
	}

	if *includeID {
		ids, _, err := cassandraIndex.LookupIDs([]labels.Labels{
			{
				{Name: "__metric_id__", Value: strconv.FormatInt(int64(metricsIDs[1]), 10)},
				{Name: "ignored", Value: "__metric_id__ win"},
			},
		})

		if err != nil {
			log.Fatalf("LookupIDs(__metric_id__ valid) failed: %v", err)
		}

		if ids[0] != metricsIDs[1] {
			log.Fatalf("LookupIDs(__metric_id__ valid) = %v, want %v", ids[0], metricsIDs[1])
		}

		_, _, err = cassandraIndex.LookupIDs([]labels.Labels{
			{
				{Name: "__metric_id__", Value: "00000000-0000-0000-0000-000000000001"},
				{Name: "ignored", Value: "__metric_id__ win"},
			},
		})
		if err == nil {
			log.Fatalf("LookupIDs(__metric_id__ invalid) succeeded. It must fail")
		}

		ids, err = cassandraIndex.Search([]*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__metric_id__", Value: strconv.FormatInt(int64(metricsIDs[1]), 10)},
			{Type: labels.MatchEqual, Name: "ignored", Value: "only_id_is_used"},
		})
		if err != nil {
			log.Fatalf("Search(__metric_id__ valid) failed: %v", err)
		}

		if len(ids) != 1 || ids[0] != metricsIDs[1] {
			log.Fatalf("Search(__metric_id__ valid) = %v, want [%v]", ids, metricsIDs[1])
		}

		ids, err = cassandraIndex.Search([]*labels.Matcher{
			{Type: labels.MatchEqual, Name: "__metric_id__", Value: strconv.FormatInt(int64(metricsIDs[2]), 10)},
			{Type: labels.MatchNotEqual, Name: "__name__", Value: "up"},
		})
		if err != nil {
			log.Fatalf("Search(__metric_id__ valid 2) failed: %v", err)
		}

		if len(ids) != 1 || ids[0] != metricsIDs[2] {
			log.Fatalf("Search(__metric_id__ valid 2) = %v, want [%v]", ids, metricsIDs[2])
		}
	}

	for i, id := range metricsIDs {
		if i == 0 {
			continue
		}

		labels, err := cassandraIndex.LookupLabels(id)

		if err != nil {
			log.Fatalf("LookupLabels(%d) failed: %v", i, err)
		}

		got := labels.Map()

		if !reflect.DeepEqual(got, metrics[i]) {
			log.Fatalf("LookupLabels(%d) = %v, want %v", i, got, metrics[i])
		}
	}

	if !*noDropTables {
		got, err := cassandraIndex.AllIDs()
		if err != nil {
			log.Fatalf("AllIDs() failed: %v", err)
		}

		gotMap := make(map[types.MetricID]int, len(got))

		for _, v := range got {
			gotMap[v] = ID2metrics[v]
		}

		if !reflect.DeepEqual(gotMap, ID2metrics) {
			log.Fatalf("AllIDs() = %v, want %v", gotMap, ID2metrics)
		}
	}
}
