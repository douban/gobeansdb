package store

func Fnv1a(buf []byte) (h uint32) {
	PRIME := uint32(0x01000193)
	h = 0x811c9dc5
	for _, b := range buf {
		h ^= uint32(int8(b))
		h = (h * PRIME)
	}
	return h
}
