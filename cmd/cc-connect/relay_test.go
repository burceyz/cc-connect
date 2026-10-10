package main

import (
	"net/http"
	"testing"
)

func TestRelayAcceptsPendingWithoutReportingFailure(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusAccepted} {
		if !relayResponseAccepted(status) {
			t.Fatalf("valid relay status %d rejected", status)
		}
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusInternalServerError} {
		if relayResponseAccepted(status) {
			t.Fatalf("failed relay status %d accepted", status)
		}
	}
}
