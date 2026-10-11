package antigravityhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestCommunicationMCPForwardsOnlyToolRequests(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requests := make(chan CommunicationRequest, 2)
	go func() {
		for range 2 {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var request CommunicationRequest
			if json.NewDecoder(conn).Decode(&request) == nil {
				requests <- request
				result := json.RawMessage(`{"tools":[]}`)
				if request.Method == "tools/call" {
					result = json.RawMessage(`{"content":[{"type":"text","text":"received"}],"isError":false}`)
				}
				_ = json.NewEncoder(conn).Encode(CommunicationResponse{Result: result})
			}
			_ = conn.Close()
		}
	}()
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"send_message","arguments":{"contact":"atlas","text":"hello"}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"resources/read","params":{"uri":"file:///private"}}`,
		`{"jsonrpc":"2.0","id":5,"method":"ping"}`,
	}, "\n")
	var output bytes.Buffer
	if err := ServeCommunication(strings.NewReader(input), &output, listener.Addr().String(), "host-token", "run-1"); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&output)
	for id := 1; id <= 5; id++ {
		var response struct {
			ID     int             `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		if response.ID != id {
			t.Fatalf("response ID=%d, want %d", response.ID, id)
		}
		if id == 4 {
			if response.Error == nil || response.Error.Code != -32601 {
				t.Fatal("filesystem resource request was not rejected")
			}
		} else if response.Error != nil || !json.Valid(response.Result) {
			t.Fatalf("invalid response for %d", id)
		}
	}
	for _, method := range []string{"tools/list", "tools/call"} {
		request := <-requests
		if request.Method != method || request.Token != "host-token" || request.Generation != "run-1" {
			t.Fatalf("invalid bridge request: %s", request.Method)
		}
		if !json.Valid(request.Params) {
			t.Fatal("missing object params")
		}
	}
}

func TestCommunicationMCPRejectsForeignTransportAndUninitializedCall(t *testing.T) {
	for _, address := range []string{"example.com:80", "192.0.2.1:80", "/tmp/api.sock", "127.0.0.1"} {
		if err := ServeCommunication(strings.NewReader(""), &bytes.Buffer{}, address, "token", "generation"); err == nil {
			t.Fatalf("accepted %q", address)
		}
	}
	var output bytes.Buffer
	err := ServeCommunication(strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"send_message"}}`), &output, "127.0.0.1:1", "token", "generation")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"error"`) {
		t.Fatal("call before initialize was not rejected")
	}
}

func TestCommunicationMCPRejectsOversizedInput(t *testing.T) {
	input := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s"}`, strings.Repeat("x", maxCommunicationInput))
	if err := ServeCommunication(strings.NewReader(input), &bytes.Buffer{}, "127.0.0.1:1", "token", "generation"); err == nil {
		t.Fatal("accepted oversized input")
	}
}
