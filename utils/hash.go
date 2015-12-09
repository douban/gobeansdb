package utils

type HashMethod func(v []byte) (h uint32)

// Bugy version of fnv1a
// 由于历史原因，最初使用了带有 bug 的 fnv1a，现在修正的代价比较大，
// 涉及到数据的迁移.
func Fnv1a(buf []byte) (h uint32) {
	PRIME := uint32(0x01000193)
	h = 0x811c9dc5
	for _, b := range buf {
		h ^= uint32(int8(b))
		h = (h * PRIME)
	}
	return h
}
