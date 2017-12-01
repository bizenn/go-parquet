package encoding

func uleb128encode(n uint32) (buf []byte) {
	if n == 0 {
		return []byte{0}
	}
	buf = make([]byte, 0, 4)
	for i := 0; i < 4; i++ {
		b := byte(n & 0x7f)
		if b == 0 {
			buf[i-1] &= 0x7f
			break
		}
		buf = append(buf, b|0x80)
		n >>= 7
	}
	return buf
}

func bitpackBools(bs []bool) (buf []byte, err error) {
	return buf, err
}
