// Package mutable handles mutable labels.
package mutable

import (
	"errors"
	"fmt"
	"log"
	"os"
	"regexp/syntax"

	"github.com/prometheus/prometheus/model/labels"
)

//nolint:gochecknoglobals
var (
	logger = log.New(os.Stdout, "[mutable] ", log.LstdFlags)

	errUnsupportedOperation = errors.New("unsupported operation")
)

// LabelProcessor can replace mutable labels by non mutable labels.
type LabelProcessor struct {
	labelProvider   LabelProvider
	tenantLabelName string
}

func NewLabelProcessor(provider LabelProvider, tenantLabelName string) *LabelProcessor {
	processor := LabelProcessor{
		labelProvider:   provider,
		tenantLabelName: tenantLabelName,
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
		if matcher.Name == lp.tenantLabelName {
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
		isMutableLabel, err := lp.labelProvider.IsMutableLabel(tenant, matcher.Name)
		if err != nil {
			return nil, err
		}

		if !isMutableLabel {
			processedMatchers = append(processedMatchers, matcher)

			continue
		}

		var newMatcher *labels.Matcher

		if matcher.Type == labels.MatchRegexp || matcher.Type == labels.MatchNotRegexp {
			newMatcher, err = lp.processMutableLabelRegex(tenant, matcher)
		} else {
			newMatcher, err = lp.processMutableLabel(tenant, matcher)
		}

		if err != nil {
			return nil, err
		}

		// We don't need to check for collisions between the new matchers and the existing ones,
		// Prometheus accepts queries with multiple labels with the same name.
		processedMatchers = append(processedMatchers, newMatcher)
	}

	return processedMatchers, nil
}

// processMutableLabel replaces a mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabel(tenant string, matcher *labels.Matcher) (*labels.Matcher, error) {
	lbls, err := lp.labelProvider.Get(tenant, matcher.Name, matcher.Value)
	if err != nil {
		return nil, err
	}

	newMatcher, err := mergeLabels(lbls, matcher.Type)
	if err != nil {
		return nil, fmt.Errorf("merge labels: %w", err)
	}

	return newMatcher, nil
}

// processMutableLabelRegex replaces a regex mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabelRegex(tenant string, matcher *labels.Matcher) (*labels.Matcher, error) {
	// Search for all matching values.
	var matchingLabels NonMutableLabels

	values, err := lp.labelProvider.AllValues(tenant, matcher.Name)
	if err != nil {
		return nil, err
	}

	for _, value := range values {
		if matcher.Matches(value) {
			lbls, err := lp.labelProvider.Get(tenant, matcher.Name, value)
			if err != nil {
				return nil, err
			}

			// We currently only support matching on the same non mutable label name.
			if matchingLabels.Name != "" && matchingLabels.Name != lbls.Name {
				errMsg := "%w: mutable label regex '%s' returned two different non mutable label names: %s and %s"

				return nil, fmt.Errorf(errMsg, errUnsupportedOperation, matcher.Name, matchingLabels.Name, lbls.Name)
			}

			matchingLabels = NonMutableLabels{
				Name:   lbls.Name,
				Values: append(matchingLabels.Values, lbls.Values...),
			}
		}
	}

	// The returned matcher is always a MatchRegexp, even if the matcher was a MatchNotRegexp,
	// because we searched for matching values.
	newMatcher, err := mergeLabels(matchingLabels, labels.MatchRegexp)
	if err != nil {
		return nil, fmt.Errorf("merge labels: %w", err)
	}

	return newMatcher, nil
}

// mergeLabels merge the labels in one matcher that matches any of the input label values.
// Example: mergeLabels(instance="server1", instance="server2") -> instance~="server1|server2".
func mergeLabels(lbls NonMutableLabels, matchType labels.MatchType) (matcher *labels.Matcher, err error) {
	regex, err := mergeRegex(lbls.Values)
	if err != nil {
		return nil, err
	}

	newMatcher, err := labels.NewMatcher(regexMatchType(matchType), lbls.Name, regex)
	if err != nil {
		return nil, err
	}

	return newMatcher, nil
}

// mergeRegex returns a regular expression matching any of the input expressions.
func mergeRegex(input []string) (string, error) {
	var err error

	allRegex := make([]*syntax.Regexp, len(input))

	for i, v := range input {
		allRegex[i], err = syntax.Parse(v, syntax.Perl)
		if err != nil {
			return "", err
		}
	}

	re := syntax.Regexp{
		Op:    syntax.OpAlternate,
		Flags: syntax.Perl,
		Sub:   allRegex,
	}

	return re.String(), nil
}

// regexMatchType returns the regex match type corresponding to the given type.
func regexMatchType(matchType labels.MatchType) labels.MatchType {
	if matchType == labels.MatchEqual || matchType == labels.MatchRegexp {
		return labels.MatchRegexp
	}

	return labels.MatchNotRegexp
}
