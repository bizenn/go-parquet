package encoding

import (
	"testing"
)

func eqvBytes(a, b []byte) bool {
	if a == nil {
		if a == nil {
			return true
		}
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestULEB128(t *testing.T) {
	testPattern := []struct {
		decoded uint32
		encoded []byte
	}{
		{0, []byte{0}},
		{0x41, []byte{0x41}},
		{0x81, []byte{0x81, 0x01}},
		{0x2001, []byte{0x81, 0x40}},
		{0x4001, []byte{0x81, 0x80, 0x01}},
		{0x100001, []byte{0x81, 0x80, 0x40}},
		{0x200001, []byte{0x81, 0x80, 0x80, 0x01}},
		{0x08000001, []byte{0x81, 0x80, 0x80, 0x40}},
		{0x10000001, []byte{0x81, 0x80, 0x80, 0x80, 0x01}},
		{0x80000001, []byte{0x81, 0x80, 0x80, 0x80, 0x08}},
		{0xffffffff, []byte{0xff, 0xff, 0xff, 0xff, 0x0f}},
	}

	for _, p := range testPattern {
		if !eqvBytes(p.encoded, uleb128encode(p.decoded)) {
			t.Errorf("Expected %x but got %x", p.encoded, uleb128encode(p.decoded))
		}
		if n, count := uleb128decode(p.encoded); n != p.decoded || count != len(p.encoded) {
			t.Errorf("Expected %b but got %b", p.decoded, n)
		}
	}
}
