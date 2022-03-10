package bprotoc

// for gen code
type FastCodec interface {
	FastRead
	FastWrite
	FastSize
}

type FastRead interface {
	FastRead(buf []byte, _type int8, number int32) (n int, err error)
}

type FastWrite interface {
	FastWrite(buf []byte) (n int)
}

type FastSize interface {
	Size() (n int)
}
