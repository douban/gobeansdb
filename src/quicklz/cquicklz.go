package quicklz

/*
#cgo CFLAGS: -I .
#include "quicklz.h"
size_t qlz_compress(const void *source, char *destination, size_t size, char *scratch_compress);
*/
import "C"
import "unsafe"
import "fmt"

const (
	CompressBufferSize   = 528400
	DecompressBufferSize = 16
)

func CCompress(src []byte) []byte {
	dst := make([]byte, len(src)+400)
	buf := make([]byte, CompressBufferSize)
	c_src := (unsafe.Pointer(&src[0]))
	c_dst := (*C.char)(unsafe.Pointer(&dst[0]))
	c_buf := (*C.char)(unsafe.Pointer(&buf[0]))
	c_size := C.qlz_compress(c_src, c_dst, C.size_t(len(src)), c_buf)
	size := int(c_size)
	return dst[:size]
}

func CDecompress(src []byte) []byte {
	dst := make([]byte, len(src)+400)
	buf := make([]byte, DecompressBufferSize)
	c_src := (*C.char)(unsafe.Pointer(&src[0]))
	c_dst := (unsafe.Pointer(&dst[0]))
	c_buf := (*C.char)(unsafe.Pointer(&buf[0]))
	c_size := C.qlz_decompress(c_src, c_dst, c_buf)
	size := int(c_size)
	return dst[:size]
}

func DecompressSafe(src []byte) (dst []byte, err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			err, ok = e.(error)
			if !ok {
				err = fmt.Errorf("decompress fail with non-error: %#v", e)
			}
		}
	}()
	sizeC := SizeCompressed(src)
	if len(src) != sizeC {
		return nil, fmt.Errorf("bad sizeCompressed, expect %d, got %d", sizeC, len(src))
	}
	sizeD := SizeDecompressed(src)
	dst = Decompress(src)
	if len(dst) != sizeD {
		return nil, fmt.Errorf("bad sizeDecompressed, expect %d, got %d", sizeD, len(dst))
	}
	return dst, nil
}

func CDecompressSafe(src []byte) (dst []byte, err error) {
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			err, ok = e.(error)
			if !ok {
				err = fmt.Errorf("decompress fail with non-error: %#v", e)
			}
		}
	}()
	c_src := (*C.char)(unsafe.Pointer(&src[0]))
	sizeC := int(C.qlz_size_compressed(c_src))
	if len(src) != sizeC {
		return nil, fmt.Errorf("bad sizeCompressed, expect %d, got %d", sizeC, len(src))
	}
	sizeD := int(C.qlz_size_decompressed(c_src))
	dst = Decompress(src)
	if len(dst) != sizeD {
		return nil, fmt.Errorf("bad sizeDecompressed, expect %d, got %d", sizeD, len(dst))
	}
	return dst, nil
}
