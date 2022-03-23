// Package mutable handles mutable labels.
package mutable

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

var (
	//nolint:gochecknoglobals
	logger = log.New(os.Stdout, "[mutable] ", log.LstdFlags)

	errInvalidMutableLabel = errors.New("invalid mutable label")
)

// labelProvider allows to get non mutable labels from a mutable label.
type labelProvider interface {
	Get(name, value string) (labels.Labels, bool)
	AllValues() []string
}

type cassandraProvider struct {
}

// LabelProcessor can replace mutable labels by non mutable labels.
type LabelProcessor struct {
	labelProvider labelProvider
}

func NewLabelProcessor(provider labelProvider) *LabelProcessor {
	processor := LabelProcessor{
		labelProvider: provider,
	}

	return &processor
}

// ProcessMutableLabels searches for mutable labels and replace them by non mutable labels.
// For example if we have a mutable label group "mygroup" which contains 'server1' and 'server2',
// the label matcher group="mygroup" becomes instance="server1|server2".
func (lp *LabelProcessor) ProcessMutableLabels(matchers []*labels.Matcher) ([]*labels.Matcher, error) {
	processedMatchers := make([]*labels.Matcher, 0, len(matchers))

	for _, matcher := range matchers {
		if !isMutableLabel(matcher.Name) {
			processedMatchers = append(processedMatchers, matcher)

			continue
		}

		var (
			newMatchers []*labels.Matcher
			err         error
		)

		if matcher.Type == labels.MatchRegexp || matcher.Type == labels.MatchNotRegexp {
			newMatchers, err = lp.processMutableLabelRegex(matcher)
		} else {
			newMatchers, err = lp.processMutableLabel(matcher)
		}

		if err != nil {
			return nil, err
		}

		// We don't need to check for collisions between the new matchers and the existing ones,
		// Prometheus accepts queries with multiple labels with the same name.
		processedMatchers = append(processedMatchers, newMatchers...)
	}

	return processedMatchers, nil
}

// processMutableLabel replaces a mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabel(matcher *labels.Matcher) ([]*labels.Matcher, error) {
	ls, ok := lp.labelProvider.Get(matcher.Name, matcher.Value)
	if !ok {
		fmt.Printf("!!! No match found for label %s", matcher.Name)

		// Don't return an error here, the label simply doesn't have a match.
		return nil, nil
	}

	newMatchers, err := mergeLabels(ls, matcher.Type)
	if err != nil {
		return nil, fmt.Errorf("merge labels: %w", err)
	}

	return newMatchers, nil
}

// processMutableLabelRegex replaces a regex mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabelRegex(matcher *labels.Matcher) ([]*labels.Matcher, error) {
	// Search for all matching values.
	var matchingLabels labels.Labels

	for _, value := range lp.labelProvider.AllValues() {
		if matcher.Matches(value) {
			ls, ok := lp.labelProvider.Get(matcher.Name, value)
			if !ok {
				logger.Printf("AllValues returned value '%s', but it was not found", value)

				continue
			}

			matchingLabels = append(matchingLabels, ls...)
		}
	}

	// The returned matcher is always a MatchRegexp, even if the matcher was a MatchNotRegexp,
	// because we searched for matching values.
	// TODO: Doesn't work if we have labels with different names:
	// Get(group1) -> instance="server1", instance="server2", job="job1"
	// Get(group2) -> instance="server3"
	// -> mergeLabels(): instance="server1|server2|server3", job="job1"
	// -> no longer matches instance="server3" because of the job.
	newMatchers, err := mergeLabels(matchingLabels, labels.MatchRegexp)
	if err != nil {
		return nil, fmt.Errorf("merge labels: %w", err)
	}

	return newMatchers, nil
}

// isMutableLabel returns whether the label is mutable.
func isMutableLabel(name string) bool {
	// TODO: We should not hardcode any value here and retrieve these labels from cassandra.
	return name == "group"
}

// mergeLabels merge the given labels in the resulting matchers.
// Example: mergeLabels(instance="server1", instance="server2", job="job1")
// -> instance~="server1|server2", job="job1"
func mergeLabels(ls labels.Labels, matchType labels.MatchType) (matchers []*labels.Matcher, err error) {
	labelsMap := make(map[string][]string)

	for _, label := range ls {
		labelsMap[label.Name] = append(labelsMap[label.Name], label.Value)
	}

	for name, values := range labelsMap {
		var matchRegex strings.Builder
		for i, value := range values {
			if i > 0 {
				matchRegex.WriteRune('|')
			}

			matchRegex.WriteString(value)
		}

		newMatcher, err := labels.NewMatcher(regexMatchType(matchType), name, matchRegex.String())
		if err != nil {
			return nil, err
		}

		matchers = append(matchers, newMatcher)
	}

	return matchers, nil
}

// regexMatchType returns the regex match type corresponding to the given type.
func regexMatchType(matchType labels.MatchType) labels.MatchType {
	if matchType == labels.MatchEqual || matchType == labels.MatchRegexp {
		return labels.MatchRegexp
	}

	return labels.MatchNotRegexp
}
