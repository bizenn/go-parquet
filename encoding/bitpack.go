package encoding

const (
	mask uint32 = 0x80000000
)

func uleb128encode(n uint32) (buf []byte) {
	if n == 0 {
		return []byte{0}
	}
	var bitwidth uint32
	for i := 0; i < 32; i++ {
		if (n & (mask >> uint32(i))) != 0 {
			bitwidth = 32 - uint32(i)
			break
		}
	}
	byteCount := (bitwidth + 6) / 7
	buf = make([]byte, byteCount, byteCount)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(n&0x7f) | 0x80
		n >>= 7
	}
	buf[len(buf)-1] &= 0x7f
	return buf
}

func uleb128decode(buf []byte) (n uint32) {
	var s uint32
	for _, b := range buf {
		n |= uint32(b&0x7f) << s
		if b&0x80 == 0 {
			break
		}
		s += 7
	}
	return n
}

func bitpackBools(bs []bool) (buf []byte, err error) {
	return buf, err
}
