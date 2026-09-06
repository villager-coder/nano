package message

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

func TestDecodeMalformed(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{"empty", nil, ErrInvalidMessage},
		{"flag only", []byte{0}, ErrInvalidMessage},
		{"invalid type", []byte{8, 0}, ErrWrongMessageType},
		{"missing route", []byte{0, 1}, ErrWrongMessage},
		{"truncated compressed route", []byte{3, 0}, ErrWrongMessage},
		{"truncated request route", []byte{1, 1, 0}, ErrWrongMessage},
		{"truncated route string", []byte{2, 3, 'a'}, ErrWrongMessage},
		{"truncated id", []byte{4, 0x80}, ErrWrongMessage},
		{"overflow id", append([]byte{4}, append(bytes.Repeat([]byte{0xff}, 9), 2)...), ErrWrongMessage},
		{"unterminated id", append([]byte{4}, bytes.Repeat([]byte{0x80}, 11)...), ErrWrongMessage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Limit capacity too: a truncated field must not read spare capacity.
			m, err := Decode(tt.data[:len(tt.data):len(tt.data)])
			if err != tt.want || m != nil {
				t.Fatalf("Decode = %v, %v; want nil, %v", m, err, tt.want)
			}
		})
	}
}

func TestMessageRoundTripBoundaries(t *testing.T) {
	for _, typ := range []Type{Request, Response, Notify, Push} {
		for _, id := range []uint64{0, 127, 128, 16383, 16384, math.MaxUint64} {
			for _, payload := range [][]byte{nil, {1, 2, 3}} {
				m := &Message{Type: typ, Data: payload}
				if typ == Request || typ == Response {
					m.ID = id
				}
				if routable(typ) {
					m.Route = strings.Repeat("x", 255)
				}
				encoded, err := Encode(m)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := Decode(encoded)
				if err != nil || decoded.Type != m.Type || decoded.ID != m.ID || decoded.Route != m.Route || !bytes.Equal(decoded.Data, m.Data) {
					t.Fatalf("round trip type=%v id=%d: %v, %v", typ, id, decoded, err)
				}
			}
		}
	}
	if _, err := Encode(&Message{Type: Notify, Route: strings.Repeat("x", 256)}); err != ErrWrongMessage {
		t.Fatalf("oversized route: %v", err)
	}
}

func TestDictionaryConcurrentAccess(t *testing.T) {
	const route = "robustness.concurrent"
	SetDictionary(map[string]uint16{route: 65000})
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				SetDictionary(map[string]uint16{fmt.Sprintf("robustness.%d.%d", worker, i): uint16(60000 + worker*100 + i)})
				data, err := Encode(&Message{Type: Notify, Route: route})
				if err != nil {
					t.Error(err)
					return
				}
				m, err := Decode(data)
				if err != nil || m.Route != route {
					t.Errorf("dictionary round trip: %v, %v", m, err)
					return
				}
				dict, _ := GetDictionary()
				delete(dict, route)
			}
		}()
	}
	wg.Wait()
	dict, _ := GetDictionary()
	if dict[route] != 65000 {
		t.Fatal("GetDictionary exposed the live map")
	}
}

func FuzzDecode(f *testing.F) {
	for _, data := range [][]byte{{}, {3, 0}, {4, 0x80}, {4, 0}, {2, 1, 'a'}, {0, 128, 1, 0}} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := Decode(data)
		if err != nil {
			return
		}
		encoded, err := Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Decode(encoded)
		if err != nil || again.Type != m.Type || again.ID != m.ID || again.Route != m.Route || !bytes.Equal(again.Data, m.Data) {
			t.Fatalf("round trip: %v, %v", again, err)
		}
	})
}
