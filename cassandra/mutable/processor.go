// Package mutable handles mutable labels.
package mutable

import (
	"errors"
	"fmt"
	"log"
	"os"
	"regexp/syntax"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
)

//nolint:gochecknoglobals
var (
	logger = log.New(os.Stdout, "[mutable] ", log.LstdFlags)

	errUnsupportedOperation = errors.New("unsupported operation")
	ErrNoResult             = errors.New("no result")
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

// ReplaceMutableLabels searches for mutable labels and replace them by non mutable labels.
// For example if we have a mutable label group "mygroup" which contains 'server1' and 'server2',
// the label matcher group="mygroup" becomes instance="server1|server2".
func (lp *LabelProcessor) ReplaceMutableLabels(matchers []*labels.Matcher) ([]*labels.Matcher, error) {
	processedMatchers := make([]*labels.Matcher, 0, len(matchers))
	tenant := lp.tenantFromMatchers(matchers)

	// Mutable labels are disabled when no tenant is found.
	if tenant == "" {
		return matchers, nil
	}

	// Search for mutable labels and replace them by non mutable labels.
	for _, matcher := range matchers {
		isMutableLabel, err := lp.IsMutableLabel(tenant, matcher.Name)
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

// IsMutableLabel returns whether the label name is mutable.
func (lp *LabelProcessor) IsMutableLabel(tenant, name string) (bool, error) {
	mutableLabelNames, err := lp.labelProvider.MutableLabelNames(tenant)
	if err != nil {
		return false, err
	}

	for _, mutName := range mutableLabelNames {
		if name == mutName {
			return true, nil
		}
	}

	return false, nil
}

func (lp *LabelProcessor) tenantFromMatchers(matchers []*labels.Matcher) string {
	for _, matcher := range matchers {
		if matcher.Name == lp.tenantLabelName {
			return matcher.Value
		}
	}

	return ""
}

// processMutableLabel replaces a mutable matcher by non mutable matchers.
func (lp *LabelProcessor) processMutableLabel(tenant string, matcher *labels.Matcher) (*labels.Matcher, error) {
	lbls, err := lp.labelProvider.GetNonMutable(tenant, matcher.Name, matcher.Value)
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
			lbls, err := lp.labelProvider.GetNonMutable(tenant, matcher.Name, value)
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

	if len(matchingLabels.Values) == 0 {
		return nil, fmt.Errorf("%w: tenant=%s, matcher=%#v", ErrNoResult, tenant, matcher)
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
	// Sort the values to have a deterministic output for testing.
	sort.Strings(lbls.Values)

	regex, err := MergeRegex(lbls.Values)
	if err != nil {
		return nil, err
	}

	newMatcher, err := labels.NewMatcher(regexMatchType(matchType), lbls.Name, regex)
	if err != nil {
		return nil, err
	}

	return newMatcher, nil
}

// MergeRegex returns a regular expression matching any of the input expressions.
func MergeRegex(input []string) (string, error) {
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

// AddMutableLabels searches for non mutable labels and add their corresponding mutable labels
// if they exist. For example if we have a mutable label group "mygroup" which contains 'server1',
// and the label instance="server1" as input, the label group="mygroup" will be added.
func (lp *LabelProcessor) AddMutableLabels(lbls labels.Labels) (labels.Labels, error) {
	// Find the tenant.
	var tenant string

	for _, label := range lbls {
		if label.Name == lp.tenantLabelName {
			tenant = label.Value

			break
		}
	}

	// Mutable labels are disabled when no tenant is found.
	if tenant == "" {
		return lbls, nil
	}

	// Search for mutable labels associated to these labels.
	for _, label := range lbls {
		newMutableLabels, err := lp.labelProvider.GetMutable(tenant, label.Name, label.Value)
		if err != nil {
			if errors.Is(err, ErrNoResult) {
				continue
			}

			return nil, err
		}

		if len(newMutableLabels) > 0 {
			lbls = append(lbls, newMutableLabels...)
		}
	}

	// Sort the labels to make sure we always return the same labels for a given input.
	sort.Sort(lbls)

	return lbls, nil
}

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (lp *LabelProcessor) MutableLabelNames(tenant string) ([]string, error) {
	return lp.labelProvider.MutableLabelNames(tenant)
}
