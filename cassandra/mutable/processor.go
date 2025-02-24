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

// Package mutable handles mutable labels.
package mutable

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
)

var (
	errUnsupportedOperation = errors.New("unsupported operation")
	errNoResult             = errors.New("no result")
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
func (lp *LabelProcessor) ReplaceMutableLabels(
	ctx context.Context,
	matchers []*labels.Matcher,
) ([]*labels.Matcher, error) {
	processedMatchers := make([]*labels.Matcher, 0, len(matchers))
	tenant := lp.tenantFromMatchers(matchers)

	// Mutable labels are disabled when no tenant is found.
	if tenant == "" {
		return matchers, nil
	}

	// Search for mutable labels and replace them by non mutable labels.
	for _, matcher := range matchers {
		isMutableLabel, err := lp.IsMutableLabel(ctx, tenant, matcher.Name)
		if err != nil {
			return nil, err
		}

		if !isMutableLabel {
			processedMatchers = append(processedMatchers, matcher)

			continue
		}

		var newMatcher *labels.Matcher

		if matcher.Type == labels.MatchRegexp || matcher.Type == labels.MatchNotRegexp {
			newMatcher, err = lp.processMutableLabelRegex(ctx, tenant, matcher)
		} else {
			newMatcher, err = lp.processMutableLabel(ctx, tenant, matcher)
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
func (lp *LabelProcessor) IsMutableLabel(ctx context.Context, tenant, name string) (bool, error) {
	mutableLabelNames, err := lp.labelProvider.MutableLabelNames(ctx, tenant)
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
func (lp *LabelProcessor) processMutableLabel(ctx context.Context,
	tenant string,
	matcher *labels.Matcher,
) (*labels.Matcher, error) {
	lbls, err := lp.labelProvider.GetNonMutable(ctx, tenant, matcher.Name, matcher.Value)
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
func (lp *LabelProcessor) processMutableLabelRegex(
	ctx context.Context,
	tenant string,
	matcher *labels.Matcher,
) (*labels.Matcher, error) {
	// Search for all matching values.
	var matchingLabels NonMutableLabels

	values, err := lp.labelProvider.AllValues(ctx, tenant, matcher.Name)
	if err != nil {
		return nil, err
	}

	for _, value := range values {
		if matcher.Matches(value) {
			lbls, err := lp.labelProvider.GetNonMutable(ctx, tenant, matcher.Name, value)
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
		return nil, fmt.Errorf("%w: tenant=%s, matcher=%#v", errNoResult, tenant, matcher)
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

// MergeRegex returns a regular expression matching any of the input strings.
func MergeRegex(input []string) (string, error) {
	var err error

	allRegex := make([]*syntax.Regexp, len(input))

	for i, v := range input {
		escaped := regexp.QuoteMeta(v)

		allRegex[i], err = syntax.Parse(escaped, syntax.Perl)
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
// It also removes any mutable labels present in the input.
func (lp *LabelProcessor) AddMutableLabels(ctx context.Context, lbls labels.Labels) (labels.Labels, error) {
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

	builder := labels.NewBuilder(lbls)

	// Search for mutable labels associated to these labels.
	for _, label := range lbls {
		isMutable, err := lp.IsMutableLabel(ctx, tenant, label.Name)
		if err != nil {
			return nil, err
		}

		// Remove mutable labels present in the input.
		// It means the metric was written with labels that later became mutable.
		if isMutable {
			builder.Del(label.Name)
		}

		newMutableLabels, err := lp.labelProvider.GetMutable(ctx, tenant, label.Name, label.Value)
		if err != nil {
			if errors.Is(err, errNoResult) {
				continue
			}

			return nil, err
		}

		for _, label := range newMutableLabels {
			builder.Set(label.Name, label.Value)
		}
	}

	return builder.Labels(), nil
}

// MutableLabelNames returns all the mutable label names possible for a tenant.
func (lp *LabelProcessor) MutableLabelNames(ctx context.Context, tenant string) ([]string, error) {
	return lp.labelProvider.MutableLabelNames(ctx, tenant)
}
