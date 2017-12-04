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

func uleb128decode(buf []byte) (n uint32, count int) {
	var s uint32
	for i, b := range buf {
		n |= uint32(b&0x7f) << s
		if b&0x80 == 0 {
			count = 1 + i
			break
		}
		s += 7
	}
	return n, count
}

func bitpackBools(bs []bool) (buf []byte, err error) {
	byteCount := (len(bs) + 7) / 8
	header := uleb128encode(uint32(byteCount<<1 | 1))
	buf = make([]byte, byteCount, byteCount)
	for i, b := range bs {
		if b {
			buf[i/8] |= 1 << uint(i%8)
		}
	}
	buf = append(header, buf...)
	return buf, err
}
