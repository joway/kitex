package bcache

import (
	"unsafe"
)

//go:noescape
//go:linkname runtime_memhash runtime.memhash
func runtime_memhash(p unsafe.Pointer, h, s uintptr) uintptr

//go:linkname runtime_memequal runtime.memequal
func runtime_memequal(a, b unsafe.Pointer, size uintptr) bool

type stringStruct struct {
	str unsafe.Pointer
	len int
}

func memhash(data []byte) uint64 {
	return uint64(runtime_memhash(unsafe.Pointer(&data), 0, uintptr(len(data))))
}

func memequal(a, b []byte) bool {
	aa := unsafe.Pointer(&a)
	bb := unsafe.Pointer(&b)
	return runtime_memequal(aa, bb, uintptr(len(a)))
}

type cursor struct {
	start uint64
	size  uint64
}

type bcache struct {
	arena  []byte
	offset uint64
	size   uint64
	table  map[uint64]cursor
}

func NewBCache(arena []byte) *bcache {
	return &bcache{
		arena:  arena,
		offset: 0,
		size:   uint64(len(arena)),
		table:  map[uint64]cursor{},
	}
}

func (s *bcache) New(data []byte) (sb []byte) {
	h := memhash(data)
	l := uint64(len(data))
	it, ok := s.table[h]
	if ok {
		sb = s.arena[it.start : it.start+it.size]
		return sb
	}
	if s.offset+l <= s.size {
		sb = s.arena[s.offset : s.offset+l]
		s.table[h] = cursor{start: s.offset, size: l}
		s.offset += l
		copy(sb, data)
		return sb
	}
	sb = make([]byte, len(data))
	copy(sb, data)
	return sb
}
