package channel

import (
	"encoding/json"
	"testing"
)

// These tests assert the DOCUMENTED handshake structs round-trip the
// representative sample JSON from CHANNELS_SCHEMA.md / the spike doc. They
// pin the schema-of-record (the live channel.go builds the same frames).

const sampleInitializeParams = `{
  "protocolVersion": "2025-06-18",
  "capabilities": {
    "experimental": {
      "claude/channel": {}
    }
  },
  "clientInfo": {
    "name": "claude-code",
    "version": "2.1.80"
  }
}`

const sampleInitializeResult = `{
  "protocolVersion": "2025-06-18",
  "capabilities": {
    "experimental": {
      "claude/channel": {}
    },
    "tools": {}
  },
  "serverInfo": {
    "name": "peerbus-cc-adapter",
    "version": "1"
  }
}`

const samplePushNotification = `{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "content": "message body surfaced to the session",
    "meta": {
      "from": "peer-name",
      "source": "peer-bus",
      "msg_id": "01J9Z000000000000000000000"
    }
  }
}`

// malformed: a push frame missing the required "method" field.
const malformedPushNotification = `{
  "jsonrpc": "2.0",
  "params": { "content": "x" }
}`

func TestHandshakeDocumentedSchema(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		decode  func([]byte) (any, error)
		wantErr bool
		check   func(t *testing.T, v any)
	}{
		{
			name: "initialize params round-trips and carries channel capability",
			in:   sampleInitializeParams,
			decode: func(b []byte) (any, error) {
				var p InitializeParams
				err := json.Unmarshal(b, &p)
				return p, err
			},
			check: func(t *testing.T, v any) {
				p := v.(InitializeParams)
				if p.ProtocolVersion != MCPProtocolVersion {
					t.Fatalf("protocolVersion = %q, want %q", p.ProtocolVersion, MCPProtocolVersion)
				}
				if _, ok := p.Capabilities.Experimental[ChannelCapabilityKey]; !ok {
					t.Fatalf("missing experimental[%q] capability", ChannelCapabilityKey)
				}
				if p.ClientInfo.Name != "claude-code" {
					t.Fatalf("clientInfo.name = %q, want claude-code", p.ClientInfo.Name)
				}
			},
		},
		{
			name: "initialize result round-trips and advertises channel + tools capability",
			in:   sampleInitializeResult,
			decode: func(b []byte) (any, error) {
				var r InitializeResult
				err := json.Unmarshal(b, &r)
				return r, err
			},
			check: func(t *testing.T, v any) {
				r := v.(InitializeResult)
				if _, ok := r.Capabilities.Experimental[ChannelCapabilityKey]; !ok {
					t.Fatalf("result missing experimental[%q]", ChannelCapabilityKey)
				}
				if len(r.Capabilities.Tools) == 0 {
					t.Fatalf("result missing tools capability")
				}
				if r.ServerInfo.Name != "peerbus-cc-adapter" {
					t.Fatalf("serverInfo.name = %q, want peerbus-cc-adapter", r.ServerInfo.Name)
				}
			},
		},
		{
			name: "push notification round-trips with content and string meta",
			in:   samplePushNotification,
			decode: func(b []byte) (any, error) {
				var n ChannelPushNotification
				err := json.Unmarshal(b, &n)
				return n, err
			},
			check: func(t *testing.T, v any) {
				n := v.(ChannelPushNotification)
				if n.Method != PushMethod {
					t.Fatalf("method = %q, want %q", n.Method, PushMethod)
				}
				if n.Params.Content != "message body surfaced to the session" {
					t.Fatalf("unexpected content: %q", n.Params.Content)
				}
				if n.Params.Meta["source"] != "peer-bus" {
					t.Fatalf("meta.source = %q, want peer-bus", n.Params.Meta["source"])
				}
				if n.Params.Meta["msg_id"] == "" {
					t.Fatalf("meta.msg_id missing")
				}
			},
		},
		{
			name: "malformed push (no method) is rejected as not a valid push",
			in:   malformedPushNotification,
			decode: func(b []byte) (any, error) {
				var n ChannelPushNotification
				if err := json.Unmarshal(b, &n); err != nil {
					return n, err
				}
				if n.Method != PushMethod {
					return n, errInvalidPush
				}
				return n, nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.decode([]byte(tt.in))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (value=%+v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Round-trip: re-marshal then re-decode must yield an equal value.
			b, merr := json.Marshal(got)
			if merr != nil {
				t.Fatalf("re-marshal failed: %v", merr)
			}
			got2, derr := tt.decode(b)
			if derr != nil {
				t.Fatalf("re-decode failed: %v", derr)
			}
			if tt.check != nil {
				tt.check(t, got)
				tt.check(t, got2)
			}
		})
	}
}

// errInvalidPush is a sentinel for the malformed-sample error case.
var errInvalidPush = &pushError{"frame is not a valid claude/channel push (missing/invalid method)"}

type pushError struct{ msg string }

func (e *pushError) Error() string { return e.msg }
