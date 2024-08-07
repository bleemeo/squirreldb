package remotestorage_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bleemeo/squirreldb/api/remotestorage"
	"github.com/bleemeo/squirreldb/types"

	"github.com/prometheus/client_golang/prometheus"
)

// TestAppenderInvalidRequest tests that the appender still works after a lot of invalid requests.
// It ensures that the remoteWriteGate is correctly released with invalid inputs.
func TestAppenderInvalidRequest(t *testing.T) {
	const maxConcurrentWrite = 5

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	app := remotestorage.New(nil, nil, maxConcurrentWrite, "", mockMutableLAbel{}, true, prometheus.NewRegistry())

	// Send some requests with a missing tenant header.
	for range 2 * maxConcurrentWrite {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9201/api/v1/write", nil)
		subCtx := types.WrapContext(ctx, request)

		appender := app.Appender(subCtx)

		_, err := appender.Append(0, nil, 0, 0)
		if !errors.Is(err, remotestorage.ErrMissingTenantHeader) {
			t.Fatalf("Expected ErrMissingTenantHeader, got %s", err)
		}
	}

	// Send some requests with an invalid TTL header.
	for range 2 * maxConcurrentWrite {
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost:9201/api/v1/write", nil)
		request.Header.Set(types.HeaderTenant, "my_tenant")
		request.Header.Set(types.HeaderTimeToLive, "invalid_ttl")

		subCtx := types.WrapContext(ctx, request)

		appender := app.Appender(subCtx)

		_, err := appender.Append(0, nil, 0, 0)
		if !errors.Is(err, remotestorage.ErrParseTTLHeader) {
			t.Fatalf("Expected ErrMissingTenantHeader, got %s", err)
		}
	}
}

type mockMutableLAbel struct{}

func (mockMutableLAbel) IsMutableLabel(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}
