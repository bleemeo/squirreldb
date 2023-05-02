package mutable

import (
	"context"
	"squirreldb/types"
	"time"
)

type notImplementedVerifier struct{}

func (v notImplementedVerifier) WithNow(now time.Time) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithDoFix(enable bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithLock(enable bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictExpiration(enable bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithStrictMetricCreation(enable bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) WithPedanticExpiration(enable bool) types.IndexVerifier {
	return v
}

func (v notImplementedVerifier) Verify(ctx context.Context) (hadIssue bool, err error) {
	return false, errNotImplemented
}
