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

// LabelProcessor can replace mutable labels by non mutable labels.
type LabelProcessor struct {
	labelProvider LabelProvider
}

func NewLabelProcessor(provider LabelProvider) *LabelProcessor {
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

	// Find the tenant.
	var tenant string

	for _, matcher := range matchers {
		if lp.labelProvider.IsTenantLabel(matcher.Name) {
			tenant = matcher.Value

			break
		}
	}

	if tenant == "" {
		logger.Printf("No tenant found in matchers %v", matchers)

		return matchers, nil
	}

	// Search for mutable labels and replace them by non mutable labels.
	for _, matcher := range matchers {
		if !lp.labelProvider.IsMutableLabel(matcher.Name) {
			processedMatchers = append(processedMatchers, matcher)

			continue
		}

		var (
			newMatchers []*labels.Matcher
			err         error
		)

		if matcher.Type == labels.MatchRegexp || matcher.Type == labels.MatchNotRegexp {
			newMatchers, err = lp.processMutableLabelRegex(tenant, matcher)
		} else {
			newMatchers, err = lp.processMutableLabel(tenant, matcher)
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
func (lp *LabelProcessor) processMutableLabel(tenant string, matcher *labels.Matcher) ([]*labels.Matcher, error) {
	ls, err := lp.labelProvider.Get(tenant, matcher.Name, matcher.Value)
	if err != nil {
		return nil, err
	}

	newMatchers, err := mergeLabels(ls, matcher.Type)
	if err != nil {
		return nil, fmt.Errorf("merge labels: %w", err)
	}

	return newMatchers, nil
}

// processMutableLabelRegex replaces a regex mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabelRegex(tenant string, matcher *labels.Matcher) ([]*labels.Matcher, error) {
	// Search for all matching values.
	var matchingLabels labels.Labels

	values, err := lp.labelProvider.AllValues(tenant, matcher.Name)
	if err != nil {
		return nil, err
	}

	for _, value := range values {
		if matcher.Matches(value) {
			ls, err := lp.labelProvider.Get(tenant, matcher.Name, value)
			if err != nil {
				return nil, err
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
