package main

import (
	"log"
	"reflect"
	"squirreldb/cassandra/index"

	gouuid "github.com/gofrs/uuid"
	"github.com/prometheus/prometheus/prompb"
)

func test(cassandraIndex *index.CassandraIndex) { //nolint: gocognit
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
		Matchers        []*prompb.LabelMatcher
		MatchingMetrics []int
	}{
		{
			Name: "eq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "up",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "eq-eq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "eq-neq",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_cpu_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "mode",
					Value: "user",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "eq-nolabel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "eq-label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NEQ,
					Name:  "environment",
					Value: "",
				},
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "u.",
				},
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "re-re",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_cpu_.*",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "mode",
					Value: "^u.*",
				},
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "re-nre",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_(cpu|disk)_seconds_total",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "mode",
					Value: "u\\wer",
				},
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "re-re_nolabel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "re-re_label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re-re*",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{7, 8, 9, 10},
		},
		{
			Name: "re-nre*",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes$",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: ".*",
				},
			},
			MatchingMetrics: []int{},
		},
		{
			Name: "eq-nre_empty_and_devel",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "(|devel)",
				},
			},
			MatchingMetrics: []int{9, 10},
		},
		{
			Name: "eq-nre-eq same label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_NRE,
					Name:  "environment",
					Value: "^$",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "devel",
				},
			},
			MatchingMetrics: []int{8},
		},
		{
			Name: "eq-eq-no_label",
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "__name__",
					Value: "node_filesystem_avail_bytes",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "environment",
					Value: "production",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "userID",
					Value: "",
				},
			},
			MatchingMetrics: []int{9},
		},
	}

	metricsUUID := make([]gouuid.UUID, len(metrics))
	UUIDmetrics := make(map[gouuid.UUID]int, len(metrics))

	for i, labels := range metrics {
		if i == 0 {
			continue
		}

		uuid, ttl, err := cassandraIndex.LookupUUID(map2Labels(labels))
		if err != nil {
			log.Fatalf("LookupUUID(%v) failed: %v", labels, err)
		}

		if ttl != int64(defaultTimeToLive.Seconds()) {
			log.Fatalf("TTL = %d, want default TTL (%d)", ttl, *defaultTimeToLive)
		}

		metricsUUID[i] = uuid
		UUIDmetrics[uuid] = i
	}

	for _, tt := range tests {
		wantUUIDToIndex := make(map[gouuid.UUID]int, len(tt.MatchingMetrics))

		for _, m := range tt.MatchingMetrics {
			wantUUIDToIndex[metricsUUID[m]] = m
		}

		uuids, err := cassandraIndex.Search(tt.Matchers)

		if err != nil {
			log.Fatalf("Search(%s) failed: %v", tt.Name, err)
		}

		gotUUIDToIndex := make(map[gouuid.UUID]int, len(uuids))

		for _, uuid := range uuids {
			gotUUIDToIndex[uuid] = UUIDmetrics[uuid]
		}

		if !reflect.DeepEqual(wantUUIDToIndex, gotUUIDToIndex) {
			log.Fatalf("Search(%s) = %v, want %v", tt.Name, gotUUIDToIndex, wantUUIDToIndex)
		}
	}

	if *includeUUID {
		uuid, _, err := cassandraIndex.LookupUUID([]*prompb.Label{
			{Name: "__uuid__", Value: metricsUUID[1].String()},
			{Name: "ignored", Value: "__uuid__ win"},
		})

		if err != nil {
			log.Fatalf("LookupUUID(__uuid__ valid) failed: %v", err)
		}

		if uuid != metricsUUID[1] {
			log.Fatalf("LookupUUID(__uuid__ valid) = %v, want %v", uuid, metricsUUID[1])
		}

		_, _, err = cassandraIndex.LookupUUID([]*prompb.Label{
			{Name: "__uuid__", Value: "00000000-0000-0000-0000-000000000001"},
			{Name: "ignored", Value: "__uuid__ win"},
		})
		if err == nil {
			log.Fatalf("LookupUUID(__uuid__ invalid) succeeded. It must fail")
		}

		uuids, err := cassandraIndex.Search([]*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__uuid__", Value: metricsUUID[1].String()},
			{Type: prompb.LabelMatcher_EQ, Name: "ignored", Value: "only_uuid_is_used"},
		})
		if err != nil {
			log.Fatalf("Search(__uuid__ valid) failed: %v", err)
		}

		if len(uuids) != 1 || uuids[0] != metricsUUID[1] {
			log.Fatalf("Search(__uuid__ valid) = %v, want [%v]", uuids, metricsUUID[1])
		}

		uuids, err = cassandraIndex.Search([]*prompb.LabelMatcher{
			{Type: prompb.LabelMatcher_EQ, Name: "__uuid__", Value: metricsUUID[2].String()},
			{Type: prompb.LabelMatcher_NEQ, Name: "__name__", Value: "up"},
		})
		if err != nil {
			log.Fatalf("Search(__uuid__ valid 2) failed: %v", err)
		}

		if len(uuids) != 1 || uuids[0] != metricsUUID[2] {
			log.Fatalf("Search(__uuid__ valid 2) = %v, want [%v]", uuids, metricsUUID[2])
		}
	}

	for i, uuid := range metricsUUID {
		if i == 0 {
			continue
		}

		labels, err := cassandraIndex.LookupLabels(uuid)

		if err != nil {
			log.Fatalf("LookupLabels(%d) failed: %v", i, err)
		}

		got := labels2Map(labels)

		if !reflect.DeepEqual(got, metrics[i]) {
			log.Fatalf("LookupLabels(%d) = %v, want %v", i, got, metrics[i])
		}
	}

	if !*noDropTables {
		got, err := cassandraIndex.AllUUIDs()
		if err != nil {
			log.Fatalf("AllUUIDs() failed: %v", err)
		}

		gotMap := make(map[gouuid.UUID]int, len(got))

		for _, v := range got {
			gotMap[v] = UUIDmetrics[v]
		}

		if !reflect.DeepEqual(gotMap, UUIDmetrics) {
			log.Fatalf("AllUUIDs() = %v, want %v", gotMap, UUIDmetrics)
		}
	}
}
