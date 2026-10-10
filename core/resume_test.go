package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestResumeUnavailableError_PreservesWrappedCause(t *testing.T) {
	err := fmt.Errorf("start session: %w", &ResumeUnavailableError{Err: context.Canceled})
	var unavailable *ResumeUnavailableError
	if !errors.As(err, &unavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("resume failure cannot be detected through wrapping: %v", err)
	}
	if unavailable.Error() != context.Canceled.Error() {
		t.Fatalf("underlying failure was lost: %v", unavailable)
	}
	if (&ResumeUnavailableError{}).Error() == "" {
		t.Fatal("resume failure without a cause has no diagnostic")
	}
}
