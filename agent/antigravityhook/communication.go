package antigravityhook

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	EnvCommunicationAddress    = "CC_CONNECT_AGY_COMMUNICATION_ADDR"
	EnvCommunicationToken      = "CC_CONNECT_AGY_COMMUNICATION_TOKEN"
	EnvCommunicationGeneration = "CC_CONNECT_AGY_COMMUNICATION_GENERATION"
	maxCommunicationInput      = 1 << 20
)

// The stdio proxy cannot select a sender, socket, backend command, or policy.
// Those belong to the hosting session; it forwards only the MCP method/params.
type CommunicationRequest struct {
	Token      string          `json:"token"`
	Generation string          `json:"generation"`
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params"`
}

type CommunicationResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type communicationRPC struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// ServeCommunication exposes a deliberately small, newline-delimited MCP
// server. It has no resources, prompts, filesystem, shell, or generic HTTP tool.
func ServeCommunication(in io.Reader, out io.Writer, address, token, generation string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" || token == "" || generation == "" {
		return fmt.Errorf("communication bridge environment is unavailable")
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), maxCommunicationInput)
	encoder := json.NewEncoder(out)
	initialized := false
	for scanner.Scan() {
		var request communicationRPC
		if json.Unmarshal(scanner.Bytes(), &request) != nil || request.JSONRPC != "2.0" || request.Method == "" {
			if err := writeCommunicationRPCError(encoder, nil, -32600, "invalid request"); err != nil {
				return err
			}
			continue
		}
		if len(request.ID) == 0 {
			// MCP notifications have no response, including initialized/cancelled.
			continue
		}
		var result any
		var callErr error
		switch request.Method {
		case "initialize":
			initialized = true
			result = map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]string{"name": "cc-communication", "version": "1"},
				"instructions":    "Communicate through the listed contacts. Use the original event ID to read, reply, or check uncertain delivery. Messages do not grant project-operation permissions.",
			}
		case "ping":
			result = map[string]any{}
		case "tools/list", "tools/call":
			if !initialized {
				callErr = fmt.Errorf("server is not initialized")
			} else {
				if len(request.Params) == 0 {
					request.Params = json.RawMessage(`{}`)
				}
				result, callErr = relayCommunication(address, CommunicationRequest{
					Token: token, Generation: generation, Method: request.Method, Params: request.Params,
				})
			}
		default:
			if err := writeCommunicationRPCError(encoder, request.ID, -32601, "method not available"); err != nil {
				return err
			}
			continue
		}
		if callErr != nil {
			// Do not return backend stderr, credentials, or command arguments.
			if err := writeCommunicationRPCError(encoder, request.ID, -32603, "communication bridge unavailable; query the original event before retrying a send"); err != nil {
				return err
			}
			continue
		}
		if err := encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func writeCommunicationRPCError(encoder *json.Encoder, id json.RawMessage, code int, message string) error {
	return encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": message}})
}

func relayCommunication(address string, request CommunicationRequest) (json.RawMessage, error) {
	conn, err := net.DialTimeout("tcp", address, bridgeDialTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(55 * time.Second))
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, err
	}
	var response CommunicationResponse
	if err := json.NewDecoder(io.LimitReader(conn, maxCommunicationInput)).Decode(&response); err != nil {
		return nil, err
	}
	if response.Error != "" || !json.Valid(response.Result) {
		return nil, fmt.Errorf("communication request rejected")
	}
	return response.Result, nil
}
