// Package quicklz implements QuickLZ compress
/*
Translation of http://www.quicklz.com/QuickLZ.java

Licensed under the GPL, like the original.
*/
package quicklz

const (
	// Streaming mode not supported
	QLZ_STREAMING_BUFFER = 0

	// Bounds checking not supported. Use try...catch instead
	QLZ_MEMORY_SAFE = 0

	QLZ_VERSION_MAJOR    = 1
	QLZ_VERSION_MINOR    = 5
	QLZ_VERSION_REVISION = 0

	// Decrease QLZ_POINTERS_3 to increase compression speed of level 3. Do not
	// edit any other constants!
	HASH_VALUES            = 4096
	MINOFFSET              = 2
	UNCONDITIONAL_MATCHLEN = 6
	UNCOMPRESSED_END       = 4
	CWORD_LEN              = 4
	DEFAULT_HEADERLEN      = 9
	QLZ_POINTERS_1         = 1
	QLZ_POINTERS_3         = 16
)

func headerLen(source []byte) int {
	if (source[0] & 2) == 2 {
		return 9
	}
	return 3
}

func sizeDecompressed(source []byte) int {
	if headerLen(source) == 9 {
		return fastRead(source, 5, 4)
	}
	return fastRead(source, 2, 1)

}

func sizeCompressed(source []byte) int {
	if headerLen(source) == 9 {
		return fastRead(source, 1, 4)
	}
	return fastRead(source, 1, 1)
}

func fastRead(a []byte, i, numbytes int) int {
	l := 0
	for j := 0; j < numbytes; j++ {
		l |= int(a[i+j]) << (uint(j) * 8)
	}
	return l
}

func fastWrite(a []byte, i, value, numbytes int) {
	for j := 0; j < numbytes; j++ {
		a[i+j] = byte(value >> (uint(j) * 8))
	}
}

func writeHeader(dst []byte, level int, compressible bool, sizeCompressed int, sizeDecompressed int) {
	var cbit byte
	if compressible {
		cbit = 1
	}
	dst[0] = byte(2 | cbit)
	dst[0] |= byte(level << 2)
	dst[0] |= (1 << 6)
	dst[0] |= (0 << 4)
	fastWrite(dst, 1, sizeDecompressed, 4)
	fastWrite(dst, 5, sizeCompressed, 4)
}

func Compress(source []byte, level int) []byte {
	var src int
	var dst = DEFAULT_HEADERLEN + CWORD_LEN
	var cwordVal uint32 = 0x80000000
	var cwordPtr = DEFAULT_HEADERLEN
	var destination = make([]byte, len(source)+400)
	var hashtable [][]int
	var cachetable = make([]int, HASH_VALUES)
	var hashCounter = make([]byte, HASH_VALUES)
	var d2 []byte
	var fetch = 0
	var lastMatchStart = (len(source) - UNCONDITIONAL_MATCHLEN - UNCOMPRESSED_END - 1)
	var lits = 0

	if level != 1 && level != 3 {
		panic("Go version only supports level 1 and 3")
	}

	hashtable = make([][]int, HASH_VALUES)

	hpointers := QLZ_POINTERS_1
	if level == 3 {
		hpointers = QLZ_POINTERS_3
	}

	for i := 0; i < HASH_VALUES; i++ {
		hashtable[i] = make([]int, hpointers)
	}

	if len(source) == 0 {
		return nil
	}

	if src <= lastMatchStart {
		fetch = fastRead(source, src, 3)
	}

	for src <= lastMatchStart {
		if (cwordVal & 1) == 1 {
			if src > 3*(len(source)>>2) && dst > src-(src>>5) {
				d2 = make([]byte, len(source)+DEFAULT_HEADERLEN)
				writeHeader(d2, level, false, len(source), len(source)+DEFAULT_HEADERLEN)
				copy(d2[DEFAULT_HEADERLEN:], source)
				return d2
			}

			fastWrite(destination, cwordPtr, int(cwordVal>>1)|0x80000000, 4)
			cwordPtr = dst
			dst += CWORD_LEN
			cwordVal = 0x80000000
		}

		if level == 1 {
			hash := ((fetch >> 12) ^ fetch) & (HASH_VALUES - 1)
			o := hashtable[hash][0]
			cache := cachetable[hash] ^ fetch

			cachetable[hash] = fetch
			hashtable[hash][0] = src

			if cache == 0 && hashCounter[hash] != 0 && (src-o > MINOFFSET || (src == o+1 && lits >= 3 && src > 3 && source[src] == source[src-3] && source[src] == source[src-2] && source[src] == source[src-1] && source[src] == source[src+1] && source[src] == source[src+2])) {
				cwordVal = ((cwordVal >> 1) | 0x80000000)
				if source[o+3] != source[src+3] {
					f := 3 - 2 | (hash << 4)
					destination[dst+0] = byte(f >> (0 * 8))
					destination[dst+1] = byte(f >> (1 * 8))
					src += 3
					dst += 2
				} else {
					oldSrc := src
					remaining := 255
					if ln := (len(source) - UNCOMPRESSED_END - src + 1 - 1); ln <= 255 {
						remaining = ln
					}

					src += 4
					if source[o+src-oldSrc] == source[src] {
						src++
						if source[o+src-oldSrc] == source[src] {
							src++
							for source[o+(src-oldSrc)] == source[src] && (src-oldSrc) < remaining {
								src++
							}
						}
					}

					matchlen := src - oldSrc

					hash <<= 4
					if matchlen < 18 {
						f := hash | (matchlen - 2)
						// Inline fastWrite
						destination[dst+0] = byte(f >> (0 * 8))
						destination[dst+1] = byte(f >> (1 * 8))
						dst += 2
					} else {
						f := hash | (matchlen << 16)
						fastWrite(destination, dst, f, 3)
						dst += 3
					}
				}
				lits = 0
				fetch = fastRead(source, src, 3)
			} else {
				lits++
				hashCounter[hash] = 1
				destination[dst] = source[src]
				cwordVal = (cwordVal >> 1)
				src++
				dst++
				fetch = (fetch>>8)&0xffff | int(source[src+2])<<16
			}
		} else {
			fetch = fastRead(source, src, 3)

			var o, offset2 int
			var matchlen, k, m int
			var c byte

			remaining := 255
			if ln := (len(source) - UNCOMPRESSED_END - src + 1 - 1); ln <= 255 {
				remaining = ln
			}

			hash := ((fetch >> 12) ^ fetch) & (HASH_VALUES - 1)

			c = hashCounter[hash]
			matchlen = 0
			offset2 = 0
			for k = 0; k < QLZ_POINTERS_3 && (int(c) > k || c < 0); k++ {

				o = hashtable[hash][k]
				if byte(fetch) == source[o] && byte(fetch>>8) == source[o+1] && byte(fetch>>16) == source[o+2] && o < src-MINOFFSET {
					m = 3
					for source[o+m] == source[src+m] && m < remaining {
						m++
					}
					if (m > matchlen) || (m == matchlen && o > offset2) {
						offset2 = o
						matchlen = m
					}
				}
			}

			o = offset2
			hashtable[hash][c&(QLZ_POINTERS_3-1)] = src
			c++
			hashCounter[hash] = c

			if matchlen >= 3 && src-o < 131071 {
				offset := src - o
				for u := 1; u < matchlen; u++ {
					fetch = fastRead(source, src+u, 3)
					hash = ((fetch >> 12) ^ fetch) & (HASH_VALUES - 1)
					c = hashCounter[hash]
					hashCounter[hash]++
					hashtable[hash][c&(QLZ_POINTERS_3-1)] = src + u
				}

				src += matchlen
				cwordVal = ((cwordVal >> 1) | 0x80000000)

				if matchlen == 3 && offset <= 63 {
					fastWrite(destination, dst, offset<<2, 1)
					dst++
				} else if matchlen == 3 && offset <= 16383 {
					fastWrite(destination, dst, (offset<<2)|1, 2)
					dst += 2
				} else if matchlen <= 18 && offset <= 1023 {
					fastWrite(destination, dst, ((matchlen-3)<<2)|(offset<<6)|2, 2)
					dst += 2
				} else if matchlen <= 33 {
					fastWrite(destination, dst, ((matchlen-2)<<2)|(offset<<7)|3, 3)
					dst += 3
				} else {
					fastWrite(destination, dst, ((matchlen-3)<<7)|(offset<<15)|3, 4)
					dst += 4
				}
			} else {
				destination[dst] = source[src]
				cwordVal = (cwordVal >> 1)
				src++
				dst++
			}
		}
	}

	for src <= len(source)-1 {
		if (cwordVal & 1) == 1 {
			fastWrite(destination, cwordPtr, int(cwordVal>>1)|0x80000000, 4)
			cwordPtr = dst
			dst += CWORD_LEN
			cwordVal = 0x80000000
		}

		destination[dst] = source[src]
		src++
		dst++
		cwordVal = (cwordVal >> 1)
	}
	for (cwordVal & 1) != 1 {
		cwordVal = (cwordVal >> 1)
	}
	fastWrite(destination, cwordPtr, int(cwordVal>>1)|0x80000000, CWORD_LEN)
	writeHeader(destination, level, true, len(source), dst)

	d2 = make([]byte, dst)
	copy(d2, destination)
	return d2
}

func Decompress(source []byte) []byte {
	size := sizeDecompressed(source)
	src := headerLen(source)
	var dst int
	var cwordVal = 1
	destination := make([]byte, size)
	hashtable := make([]int, 4096)
	hashCounter := make([]byte, 4096)
	lastMatchStart := size - UNCONDITIONAL_MATCHLEN - UNCOMPRESSED_END - 1
	lastHashed := -1
	var hash int
	var fetch int

	level := (source[0] >> 2) & 0x3

	if level != 1 && level != 3 {
		panic("Go version only supports level 1 and 3")
	}

	if (source[0] & 1) != 1 {
		d2 := make([]byte, size)
		copy(d2, source[headerLen(source):])
		return d2
	}

	for {
		if cwordVal == 1 {
			cwordVal = fastRead(source, src, 4)
			src += 4
			if dst <= lastMatchStart {
				if level == 1 {
					fetch = fastRead(source, src, 3)
				} else {
					fetch = fastRead(source, src, 4)
				}
			}
		}

		if (cwordVal & 1) == 1 {
			var matchlen int
			var offset2 int

			cwordVal = cwordVal >> 1

			if level == 1 {
				hash = (fetch >> 4) & 0xfff
				offset2 = hashtable[hash]

				if (fetch & 0xf) != 0 {
					matchlen = (fetch & 0xf) + 2
					src += 2
				} else {
					matchlen = int(source[src+2]) & 0xff
					src += 3
				}
			} else {
				var offset int

				if (fetch & 3) == 0 {
					offset = (fetch & 0xff) >> 2
					matchlen = 3
					src++
				} else if (fetch & 2) == 0 {
					offset = (fetch & 0xffff) >> 2
					matchlen = 3
					src += 2
				} else if (fetch & 1) == 0 {
					offset = (fetch & 0xffff) >> 6
					matchlen = ((fetch >> 2) & 15) + 3
					src += 2
				} else if (fetch & 127) != 3 {
					offset = (fetch >> 7) & 0x1ffff
					matchlen = ((fetch >> 2) & 0x1f) + 2
					src += 3
				} else {
					offset = (fetch >> 15)
					matchlen = ((fetch >> 7) & 255) + 3
					src += 4
				}
				offset2 = int(dst - offset)
			}

			destination[dst+0] = destination[offset2+0]
			destination[dst+1] = destination[offset2+1]
			destination[dst+2] = destination[offset2+2]

			for i := 3; i < matchlen; i++ {
				destination[dst+i] = destination[offset2+i]
			}
			dst += matchlen

			if level == 1 {
				fetch = fastRead(destination, lastHashed+1, 3) // destination[lastHashed + 1] | (destination[lastHashed + 2] << 8) | (destination[lastHashed + 3] << 16);
				for lastHashed < dst-matchlen {
					lastHashed++
					hash = ((fetch >> 12) ^ fetch) & (HASH_VALUES - 1)
					hashtable[hash] = lastHashed
					hashCounter[hash] = 1
					fetch = (fetch >> 8 & 0xffff) | (int(destination[lastHashed+3]) << 16)
				}
				fetch = fastRead(source, src, 3)
			} else {
				fetch = fastRead(source, src, 4)
			}
			lastHashed = dst - 1
		} else {
			if dst <= lastMatchStart {
				destination[dst] = source[src]
				dst++
				src++
				cwordVal = cwordVal >> 1

				if level == 1 {
					for lastHashed < dst-3 {
						lastHashed++
						fetch2 := fastRead(destination, lastHashed, 3)
						hash = ((fetch2 >> 12) ^ fetch2) & (HASH_VALUES - 1)
						hashtable[hash] = lastHashed
						hashCounter[hash] = 1
					}
					fetch = fetch>>8&0xffff | int(source[src+2])<<16
				} else {
					fetch = fetch>>8&0xffff | int(source[src+2])<<16 | int(source[src+3])<<24
				}
			} else {
				for dst <= size-1 {
					if cwordVal == 1 {
						src += CWORD_LEN
						cwordVal = 0x80000000
					}

					destination[dst] = source[src]
					dst++
					src++
					cwordVal = cwordVal >> 1
				}
				return destination
			}
		}
	}
}
