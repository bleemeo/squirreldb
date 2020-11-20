package main

import (
	"context"
	"log"
	"reflect"
	"squirreldb/types"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
)

func test(ctx context.Context, cassandraIndex types.Index) { //nolint: gocognit
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
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "eq-eq",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_cpu_seconds_total"),
				labels.MustNewMatcher(labels.MatchEqual, "mode", "user"),
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "eq-neq",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_cpu_seconds_total"),
				labels.MustNewMatcher(labels.MatchNotEqual, "mode", "user"),
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "eq-nolabel",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", ""),
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "eq-label",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchNotEqual, "environment", ""),
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "u."),
			},
			MatchingMetrics: []int{1, 2, 3},
		},
		{
			Name: "re-re",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_cpu_.*"),
				labels.MustNewMatcher(labels.MatchRegexp, "mode", "^u.*"),
			},
			MatchingMetrics: []int{5, 6},
		},
		{
			Name: "re-nre",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_(cpu|disk)_seconds_total"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "mode", "u\\wer"),
			},
			MatchingMetrics: []int{4},
		},
		{
			Name: "re-re_nolabel",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchRegexp, "environment", "^$"),
			},
			MatchingMetrics: []int{7},
		},
		{
			Name: "re-re_label",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_filesystem_avail_bytes$"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "environment", "^$"),
			},
			MatchingMetrics: []int{8, 9, 10},
		},
		{
			Name: "re-re*",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_filesystem_avail_bytes$"),
				labels.MustNewMatcher(labels.MatchRegexp, "environment", ".*"),
			},
			MatchingMetrics: []int{7, 8, 9, 10},
		},
		{
			Name: "re-nre*",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "node_filesystem_avail_bytes$"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "environment", ".*"),
			},
			MatchingMetrics: []int{},
		},
		{
			Name: "eq-nre_empty_and_devel",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "environment", "(|devel)"),
			},
			MatchingMetrics: []int{9, 10},
		},
		{
			Name: "eq-nre-eq same label",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "environment", "^$"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "devel"),
			},
			MatchingMetrics: []int{8},
		},
		{
			Name: "eq-eq-no_label",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_filesystem_avail_bytes"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "production"),
				labels.MustNewMatcher(labels.MatchEqual, "userID", ""),
			},
			MatchingMetrics: []int{9},
		},
	}

	metricsIDs := make([]types.MetricID, len(metrics))
	ID2metrics := make(map[types.MetricID]int, len(metrics))
	now := time.Now()

	for i, labelsMap := range metrics {
		if i == 0 {
			continue
		}

		ids, ttls, err := cassandraIndex.LookupIDs(
			context.Background(),
			[]types.LookupRequest{
				{
					Labels: labels.FromMap(labelsMap),
					Start:  now,
					End:    now,
				},
			},
		)
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

		metrics, err := cassandraIndex.Search(ctx, now, now, tt.Matchers)
		if err != nil {
			log.Fatalf("Search(%s) failed: %v", tt.Name, err)
		}

		gotIDToIndex := make(map[types.MetricID]int, metrics.Count())

		for metrics.Next() {
			m := metrics.At()
			gotIDToIndex[m.ID] = ID2metrics[m.ID]
		}

		if metrics.Err() != nil {
			log.Fatalf("Search(%s) failed: %v", tt.Name, err)
		}

		if !reflect.DeepEqual(wantIDToIndex, gotIDToIndex) {
			log.Fatalf("Search(%s) = %v, want %v", tt.Name, gotIDToIndex, wantIDToIndex)
		}
	}

	if *includeID {
		ids, _, err := cassandraIndex.LookupIDs(context.Background(), []types.LookupRequest{
			{
				Start: now,
				End:   now,
				Labels: labels.Labels{
					{Name: "__metric_id__", Value: strconv.FormatInt(int64(metricsIDs[1]), 10)},
					{Name: "ignored", Value: "__metric_id__ win"},
				},
			},
		})
		if err != nil {
			log.Fatalf("LookupIDs(__metric_id__ valid) failed: %v", err)
		}

		if ids[0] != metricsIDs[1] {
			log.Fatalf("LookupIDs(__metric_id__ valid) = %v, want %v", ids[0], metricsIDs[1])
		}

		_, _, err = cassandraIndex.LookupIDs(context.Background(), []types.LookupRequest{
			{
				Start: now,
				End:   now,
				Labels: labels.Labels{
					{Name: "__metric_id__", Value: "00000000-0000-0000-0000-000000000001"},
					{Name: "ignored", Value: "__metric_id__ win"},
				},
			},
		})
		if err == nil {
			log.Fatalf("LookupIDs(__metric_id__ invalid) succeeded. It must fail")
		}

		results, err := cassandraIndex.Search(ctx, now, now, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__metric_id__", strconv.FormatInt(int64(metricsIDs[1]), 10)),
			labels.MustNewMatcher(labels.MatchEqual, "ignored", "only_id_is_used"),
		})
		if err != nil {
			log.Fatalf("Search(__metric_id__ valid) failed: %v", err)
		}

		if results.Count() != 1 || !results.Next() {
			log.Fatalf("Search(__metric_id__ valid).Count() = %d, want 1", results.Count())
		}

		if results.At().ID != metricsIDs[1] {
			log.Fatalf("Search(__metric_id__ valid).Count() = %v, want [%v]", results.At(), metricsIDs[1])
		}

		got := results.At().Labels.Map()
		if !reflect.DeepEqual(got, metrics[1]) {
			log.Fatalf("Search(__metric_id__ valid) = %v, want %v", got, metrics[1])
		}

		results, err = cassandraIndex.Search(ctx, now, now, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__metric_id__", strconv.FormatInt(int64(metricsIDs[2]), 10)),
			labels.MustNewMatcher(labels.MatchNotEqual, "__name__", "up"),
		})
		if err != nil {
			log.Fatalf("Search(__metric_id__ valid 2) failed: %v", err)
		}

		if results.Count() != 1 || !results.Next() {
			log.Fatalf("Search(__metric_id__ valid).Count() = %d, want 1", results.Count())
		}

		if results.At().ID != metricsIDs[2] {
			log.Fatalf("Search(__metric_id__ valid).Count() = %v, want [%v]", results.At(), metricsIDs[2])
		}

		got = results.At().Labels.Map()
		if !reflect.DeepEqual(got, metrics[2]) {
			log.Fatalf("Search(__metric_id__ valid 2) = %v, want %v", got, metrics[2])
		}
	}

	if !*noDropTables {
		got, err := cassandraIndex.AllIDs(ctx, now, now)
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
