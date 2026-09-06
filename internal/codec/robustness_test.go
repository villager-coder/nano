package codec

import (
	"bytes"
	"testing"

	"github.com/lonng/nano/internal/packet"
)

func TestDecodeFragmentedPayload(t *testing.T) {
	for _, payload := range [][]byte{nil, {1}, {1, 2}, {1, 2, 3}, bytes.Repeat([]byte{7}, MaxPacketSize)} {
		encoded, err := Encode(packet.Data, payload)
		if err != nil {
			t.Fatal(err)
		}
		d := NewDecoder()
		packets, err := d.Decode(encoded[:HeadLength])
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range encoded[HeadLength:] {
			more, err := d.Decode([]byte{b})
			if err != nil {
				t.Fatal(err)
			}
			packets = append(packets, more...)
		}
		if len(packets) != 1 || !bytes.Equal(packets[0].Data, payload) {
			t.Fatalf("payload length %d: got %d packets", len(payload), len(packets))
		}
	}
}

func TestDecodeTerminalError(t *testing.T) {
	for _, header := range [][]byte{{0, 0, 0, 0}, {4, 1, 0, 1}} {
		d := NewDecoder()
		_, err := d.Decode(header)
		if err == nil {
			t.Fatal("invalid header accepted")
		}
		if packets, nextErr := d.Decode([]byte{4, 0, 0, 0}); nextErr != err || len(packets) != 0 {
			t.Fatalf("decoder resumed after framing error: %v, %v", packets, nextErr)
		}
	}
	if _, err := Encode(packet.Data, make([]byte, MaxPacketSize+1)); err != ErrPacketSizeExcced {
		t.Fatalf("oversized packet: %v", err)
	}
}

func FuzzDecoder(f *testing.F) {
	for _, data := range [][]byte{{}, {4, 0, 0, 1, 42}, {4, 0, 0, 0}, {0, 0, 0, 0}, {4, 1, 0, 1}} {
		f.Add(data, uint8(1))
	}
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		d := NewDecoder()
		step := int(chunk) + 1
		for len(data) > 0 {
			n := min(step, len(data))
			packets, err := d.Decode(data[:n])
			for _, p := range packets {
				if p.Length != len(p.Data) || p.Length > MaxPacketSize || p.Type < packet.Handshake || p.Type > packet.Kick {
					t.Fatalf("invalid decoded packet: %+v", p)
				}
			}
			if err != nil {
				if packets, nextErr := d.Decode(data[n:]); nextErr != err || len(packets) != 0 {
					t.Fatal("framing error was not terminal")
				}
				return
			}
			data = data[n:]
		}
	})
}
