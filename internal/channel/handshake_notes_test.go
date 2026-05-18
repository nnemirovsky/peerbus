package channel

import (
	"encoding/json"
	"testing"
)

// These tests assert the PROVISIONAL handshake structs round-trip the
// representative provisional sample JSON from
// docs/spikes/claude-channel-handshake.md. They guard the provisional schema
// only; they prove nothing about the real (uncaptured) Claude behavior.

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
    "name": "peerbus-adapter",
    "version": "0.0.0"
  }
}`

const samplePushNotification = `{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "channelId": "peerbus",
    "turnId": "01J9Z000000000000000000000",
    "content": [
      { "type": "text", "text": "message body surfaced to the session" }
    ],
    "metadata": {
      "from": "peer-name",
      "source": "peer-bus"
    }
  }
}`

// malformed: a push frame missing the required "method" field.
const malformedPushNotification = `{
  "jsonrpc": "2.0",
  "params": { "channelId": "peerbus" }
}`

func TestHandshakeProvisionalSchema(t *testing.T) {
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
				if p.ProtocolVersion != ProvisionalProtocolVersion {
					t.Fatalf("protocolVersion = %q, want %q", p.ProtocolVersion, ProvisionalProtocolVersion)
				}
				if _, ok := p.Capabilities.Experimental[ProvisionalChannelCapabilityKey]; !ok {
					t.Fatalf("missing experimental[%q] capability", ProvisionalChannelCapabilityKey)
				}
				if p.ClientInfo.Name != "claude-code" {
					t.Fatalf("clientInfo.name = %q, want claude-code", p.ClientInfo.Name)
				}
			},
		},
		{
			name: "initialize result round-trips and echoes channel capability",
			in:   sampleInitializeResult,
			decode: func(b []byte) (any, error) {
				var r InitializeResult
				err := json.Unmarshal(b, &r)
				return r, err
			},
			check: func(t *testing.T, v any) {
				r := v.(InitializeResult)
				if _, ok := r.Capabilities.Experimental[ProvisionalChannelCapabilityKey]; !ok {
					t.Fatalf("result missing experimental[%q]", ProvisionalChannelCapabilityKey)
				}
				if r.ServerInfo.Name != "peerbus-adapter" {
					t.Fatalf("serverInfo.name = %q, want peerbus-adapter", r.ServerInfo.Name)
				}
			},
		},
		{
			name: "push notification round-trips with content and metadata",
			in:   samplePushNotification,
			decode: func(b []byte) (any, error) {
				var n ChannelPushNotification
				err := json.Unmarshal(b, &n)
				return n, err
			},
			check: func(t *testing.T, v any) {
				n := v.(ChannelPushNotification)
				if n.Method != ProvisionalPushMethod {
					t.Fatalf("method = %q, want %q", n.Method, ProvisionalPushMethod)
				}
				if len(n.Params.Content) != 1 || n.Params.Content[0].Type != "text" {
					t.Fatalf("unexpected content blocks: %+v", n.Params.Content)
				}
				if n.Params.Metadata.Source != "peer-bus" {
					t.Fatalf("metadata.source = %q, want peer-bus", n.Params.Metadata.Source)
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
				if n.Method != ProvisionalPushMethod {
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
