package allocator

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/utils"
)

type testObject struct {
	Int         int                       // 8
	String      string                    // 16
	Child       *testSubObject            // 8
	StructList  []testSubObject           // 24
	StringList  []string                  // 24
	PointerList []*testSubObject          // 24
	PointerMap  map[string]*testSubObject // 8, *hmap
}

type testSubObject struct {
	Int    int    // 8
	String string // 16
}

func makeTestString(size int) string {
	var sb strings.Builder
	sb.Grow(size)
	for i := 0; i < size; i++ {
		sb.WriteByte('a' + byte(i%26))
	}
	return sb.String()
}

var benchDataSizes = []int{
	10,
	100,
	1000,
	10000,
}

func BenchmarkRawAlloc(b *testing.B) {
	for _, benchsize := range benchDataSizes {
		var strings = make([]string, 32) // escape to heap
		b.Run(fmt.Sprintf("size-%d", benchsize), func(b *testing.B) {
			readdata := []byte(makeTestString(benchsize))
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for idx := 0; idx < len(strings); idx++ {
					buf := make([]byte, len(readdata))
					copy(buf, readdata)
					strings[idx] = utils.SliceByteToString(buf)
				}
			}
		})
		_ = strings
	}
}

func BenchmarkAllocatorAlloc(b *testing.B) {
	for _, benchsize := range benchDataSizes {
		var strings = make([]string, 32) // escape to heap
		b.Run(fmt.Sprintf("size-%d", benchsize), func(b *testing.B) {
			readdata := makeTestString(benchsize)
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ac := NewAllocator(benchsize * len(strings))
				for idx := 0; idx < len(strings); idx++ {
					strings[idx] = ac.String(readdata)
				}
				ac.Release()
			}
		})
		_ = strings
	}
}

func BenchmarkBufferMalloc(b *testing.B) {
	b.Run("Malloc-10times", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var cache [10][]byte
			for i := 0; i < 10; i++ {
				cache[i] = make([]byte, 1024)
			}
			_ = cache
		}
	})
	b.Run("Malloc-1times", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var cache [10][]byte
			cache[0] = make([]byte, 1024*10)
			_ = cache
		}
	})
}

func BenchmarkBufferCopy(b *testing.B) {
	teststr := makeTestString(10240)
	b.Run("Copy-10times", func(b *testing.B) {
		var cache = make([]byte, 1024*10)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for i := 0; i < 10; i++ {
				copy(cache[i*1024:(i+1)*1024], teststr[i*1024:(i+1)*1024])
			}
		}
	})
	b.Run("Copy-1times", func(b *testing.B) {
		var cache = make([]byte, 1024*10)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			copy(cache, teststr)
		}
	})
}

func TestBufferReuse(t *testing.T) {
	for i := 0; i < 100; i++ {
		var buffer []byte
		var state int32
		go func() {
			buffer = make([]byte, 1024)
			runtime.SetFinalizer(&buffer[0], func(b *byte) {
				shdr := &sliceHeader{Data: unsafe.Pointer(b), Len: 1024, Cap: 1024}
				buffer = *(*[]byte)(unsafe.Pointer(shdr))
				atomic.StoreInt32(&state, 2)
			})
			atomic.StoreInt32(&state, 1)
		}()
		for atomic.LoadInt32(&state) == 0 {
			runtime.Gosched()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(1))
		buffer = nil // trigger gc
		for atomic.LoadInt32(&state) == 1 {
			runtime.GC()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(2))
		test.Assert(t, buffer != nil)
		test.DeepEqual(t, len(buffer), 1024)
	}
}

func TestRuntimeGC(t *testing.T) {
	var obj *testObject
	var wg sync.WaitGroup
	var finalized int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		size := unsafe.Sizeof(testObject{})
		test.DeepEqual(t, size, uintptr(112))
		buf := make([]byte, size)
		bhdr := (*sliceHeader)(unsafe.Pointer(&buf))
		memclrNoHeapPointers(bhdr.Data, uintptr(bhdr.Cap))
		obj = (*testObject)(bhdr.Data)
		obj.PointerMap = map[string]*testSubObject{"a": {Int: 123}}
		runtime.SetFinalizer(&buf[0], func(x *byte) {
			//test.Assert(t, x.PointerMap != nil, x.PointerMap)
			//test.DeepEqual(t, x.PointerMap["a"].Int, 123)
			t.Logf("finalized *testObject")
			atomic.StoreInt32(&finalized, 1)
		})
	}()
	wg.Wait()
	for i := 0; i < 10; i++ {
		runtime.GC()
	}
	t.Logf("access obj map")
	test.DeepEqual(t, obj.PointerMap["a"].Int, 123)
	mmap := obj.PointerMap
	//test.DeepEqual(t, mmap["a"].Int, 123)
	obj = nil
	t.Logf("set obj to nil")
	for atomic.LoadInt32(&finalized) == 0 {
		runtime.GC()
	}
	test.DeepEqual(t, mmap["a"].Int, 123)
}

func TestAllocator(t *testing.T) {
	ac := NewAllocator(1024)

	bytes := []byte("hello world")
	test.DeepEqual(t, ac.Bytes(bytes), bytes)
	str := "hello world"
	test.DeepEqual(t, ac.String(str), str)

	object := New[testObject](ac)
	test.DeepEqual(t, object.String, "")
	object.String = "123"
	test.DeepEqual(t, object.String, "123")

	slice := NewSlice[string](ac, 26)
	for i := 0; i < 26; i++ {
		slice[i] = string(rune('a' + i))
	}
	for i := range slice {
		test.DeepEqual(t, slice[i], string('a'+byte(i)))
	}

	mmap := NewMap[string, string](ac, 3)
	for i := 0; i < 26; i++ {
		k, v := string(rune('a'+i)), string(rune('z'-i))
		mmap[k] = v
	}
	test.DeepEqual(t, len(mmap), 26)
	for k, v := range mmap {
		test.DeepEqual(t, int(k[0]+v[0]), 219)
	}
}

func TestAllocatorLifecycle(t *testing.T) {
	for i := 0; i < 100; i++ {
		var object *testObject
		var objectField map[string]*testSubObject
		var objectFiledElement *testSubObject
		var state int32
		go func() {
			ac := NewAllocator(1024, WithFinalizer(func() {
				atomic.StoreInt32(&state, -1)
				//t.Logf("allocator finalizer finished")
			}))
			object = New[testObject](ac)
			object.PointerList = NewSlice[*testSubObject](ac, 1)
			object.PointerList[0] = new(testSubObject) // should new object natively
			object.PointerList[0].String = "123"
			object.PointerMap = NewMap[string, *testSubObject](ac, 1)
			object.PointerMap["a"] = new(testSubObject) // should new object natively
			object.PointerMap["a"].String = "123"

			objectField = object.PointerMap
			objectFiledElement = object.PointerList[0]
			runtime.SetFinalizer(objectFiledElement, func(o interface{}) {
				atomic.StoreInt32(&state, -2)
			})
			atomic.StoreInt32(&state, 1)
		}()
		// wait for alloc
		for atomic.LoadInt32(&state) == 0 {
			runtime.Gosched()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(1))

		// nothing should happen
		object.PointerList = nil
		object.PointerMap = nil
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(1))
		test.DeepEqual(t, len(objectField), 1)
		test.DeepEqual(t, objectField["a"].String, "123")

		// nothing should happen
		object = nil
		objectField = nil
		for i := 0; i < 10; i++ {
			runtime.GC()
			runtime.Gosched()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(-1))
		test.DeepEqual(t, objectFiledElement.String, "123")

		// field element should be gc
		objectFiledElement = nil
		for atomic.LoadInt32(&state) == int32(-1) {
			runtime.GC()
		}
		test.DeepEqual(t, atomic.LoadInt32(&state), int32(-2))
	}
}

func TestAllocatorMapField(t *testing.T) {
	type temp struct {
		Map map[string]string
	}
	ac := NewAllocator(1024)
	object := New[temp](ac)
	test.DeepEqual(t, len(object.Map), 0)
	object.Map = NewMap[string, string](ac, 1)
	test.DeepEqual(t, len(object.Map), 0)
	object.Map["a"] = "1"
	test.DeepEqual(t, len(object.Map), 1)
	object.Map["a"] = "2"
	test.DeepEqual(t, len(object.Map), 1)
	object.Map["b"] = "2"
	test.DeepEqual(t, len(object.Map), 2)
}

func allocObject(ac Allocator) *testObject {
	object := New[testObject](ac)

	object.Int = 123
	object.String = ac.String("123")

	object.Child = New[testSubObject](ac)
	object.Child.Int = 123
	object.Child.String = ac.String("123")

	object.StructList = NewSlice[testSubObject](ac, 1)
	object.StructList[0].Int = 123
	object.StructList[0].String = "123"

	object.StringList = NewSlice[string](ac, 1)
	object.StringList[0] = "123"

	object.PointerList = NewSlice[*testSubObject](ac, 1)
	object.PointerList[0] = new(testSubObject) // should new object natively
	object.PointerList[0].Int = 123
	object.PointerList[0].String = "123"

	object.PointerMap = NewMap[string, *testSubObject](ac, 1)
	object.PointerMap["a"] = new(testSubObject) // should new object natively
	object.PointerMap["a"].Int = 123
	object.PointerMap["a"].String = "123"

	return object
}
