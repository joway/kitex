package nphttp2

import (
	"sync"

	"github.com/cloudwego/kitex/pkg/remote"
)

type grpcConn interface {
	Read(b []byte) (n int, err error)
	WriteFrame(hdr, body []byte) (n int, err error)
}

type grpcBuffer struct {
	conn       grpcConn
	rbuf       []byte // for read
	whdr, wbuf []byte // for write
	woff       int
}

var _ remote.GRPCBuffer = (*grpcBuffer)(nil)
var bufferPool = sync.Pool{
	New: func() interface{} {
		return &grpcBuffer{
			rbuf: make([]byte, 0, 4096),
			wbuf: make([]byte, 0, 4096),
			whdr: make([]byte, 0, 8),
		}
	},
}

func newGrpcBuffer(conn grpcConn) *grpcBuffer {
	buf := bufferPool.Get().(*grpcBuffer)
	buf.conn = conn
	return buf
}

func (s *grpcBuffer) growRbuf(n int) {
	capacity := cap(s.rbuf)
	if capacity >= n {
		return
	}
	buf := make([]byte, 0, 2*capacity+n)
	s.rbuf = buf
}

func (s *grpcBuffer) growWbuf(n int) {
	if cap(s.wbuf[s.woff:]) >= n {
		return
	}
	buf := make([]byte, len(s.wbuf), 2*cap(s.wbuf)+n)
	copy(buf, s.wbuf)
	s.wbuf = buf
}

func (s *grpcBuffer) Next(n int) (p []byte, err error) {
	s.growRbuf(n)
	_, err = s.conn.Read(s.rbuf[:n])
	if err != nil {
		return nil, err
	}
	return s.rbuf[:n], nil
}

func (s *grpcBuffer) MallocHeader(n int) (buf []byte, err error) {
	// TODO: check len
	s.whdr = s.whdr[:n]
	return s.whdr, nil
}

func (s *grpcBuffer) Malloc(n int) (buf []byte, err error) {
	s.growWbuf(n)
	buf = s.wbuf[s.woff : s.woff+n]
	s.woff += n
	s.wbuf = s.wbuf[:s.woff]
	return buf, nil
}

func (s *grpcBuffer) WriteHeader(buf []byte) (err error) {
	s.whdr = buf
	return nil
}

func (s *grpcBuffer) Write(buf []byte) (err error) {
	s.wbuf = buf
	return nil
}

func (s *grpcBuffer) Flush() (err error) {
	_, err = s.conn.WriteFrame(s.whdr, s.wbuf)
	return err
}

func (s *grpcBuffer) Release(e error) (err error) {
	s.rbuf = s.rbuf[:0]
	s.whdr = s.whdr[:0]
	s.wbuf = s.wbuf[:0]
	s.woff = 0
	bufferPool.Put(s)
	return e
}
