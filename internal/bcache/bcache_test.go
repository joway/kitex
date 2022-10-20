package bcache

import (
	"fmt"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func makeStr(size int) string {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte('a' + i%26)
	}
	return string(buf)
}

var wordSizes = []int{8, 16, 64, 512, 1024}

func TestBCache(t *testing.T) {
	bc := NewBCache(make([]byte, 1024))
	round := 10
	for r := 0; r < round; r++ {
		for _, size := range wordSizes {
			str := makeStr(size)
			sb := bc.New([]byte(str))
			test.DeepEqual(t, string(sb), str)
		}
	}
}

func BenchmarkBCache(b *testing.B) {
	for _, size := range wordSizes {
		s := makeStr(size)
		sb := []byte(s)
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			bc := NewBCache(make([]byte, 1024*1024))
			for i := 0; i < b.N; i++ {
				sb2 := bc.New(sb)
				_ = sb2
			}
		})
	}
}

func BenchmarkNoCache(b *testing.B) {
	for _, size := range wordSizes {
		s := makeStr(size)
		sb := []byte(s)
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sb2 := make([]byte, len(sb))
				copy(sb2, sb)
				_ = sb2
			}
		})
	}
}

func BenchmarkMemHash(b *testing.B) {
	for _, size := range wordSizes {
		s := makeStr(size)
		sb := []byte(s)
		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				memhash(sb)
			}
		})
	}
}
