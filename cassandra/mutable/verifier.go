package mutable

import (
	"context"
	"squirreldb/types"
	"time"
)

type notImplementedVerifier struct{}

func (v notImplementedVerifier) WithNow(_ time.Time) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithDoFix(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithLock(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictExpiration(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictMetricCreation(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithPedanticExpiration(_ bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) Verify(_ context.Context) (hadIssue bool, err error) {
	return false, errNotImplemented
}
