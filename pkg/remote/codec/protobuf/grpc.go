/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package protobuf

import (
	"context"
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/cloudwego/kitex/pkg/remote"
)

// gogoproto generate
type marshaler interface {
	MarshalTo(data []byte) (n int, err error)
	Size() int
}

type protobufV2MsgCodec interface {
	XXX_Unmarshal(b []byte) error
	XXX_Marshal(b []byte, deterministic bool) ([]byte, error)
}

type GrpcCodec struct{}

func (c *GrpcCodec) Encode(ctx context.Context, message remote.Message, out remote.GRPCBuffer) (err error) {
	// 5byte + body
	data := message.Data()
	var (
		size int
	)
	switch t := data.(type) {
	case marshaler:
		size = t.Size()
		buf, err := out.Malloc(size)
		if err != nil {
			return err
		}
		if _, err = t.MarshalTo(buf); err != nil {
			return err
		}
	case protobufV2MsgCodec:
		d, err := t.XXX_Marshal(nil, true)
		if err != nil {
			return err
		}
		size = len(d)
		err = out.Write(d)
		if err != nil {
			return err
		}
	case proto.Message:
		d, err := proto.Marshal(t)
		if err != nil {
			return err
		}
		size = len(d)
		err = out.Write(d)
		if err != nil {
			return err
		}
	case protobufMsgCodec:
		d, err := t.Marshal(nil)
		if err != nil {
			return err
		}
		size = len(d)
		err = out.Write(d)
		if err != nil {
			return err
		}
	}
	hdrBuf := make([]byte, 5)
	hdrBuf[0] = 0
	binary.BigEndian.PutUint32(hdrBuf[1:5], uint32(size))
	return out.WriteHeader(hdrBuf)
}

func (c *GrpcCodec) Decode(ctx context.Context, message remote.Message, in remote.GRPCBuffer) (err error) {
	hdr, err := in.Next(5)
	if err != nil {
		return err
	}
	dLen := int(binary.BigEndian.Uint32(hdr[1:]))
	d, err := in.Next(dLen)
	if err != nil {
		return err
	}
	data := message.Data()
	switch t := data.(type) {
	case protobufV2MsgCodec:
		return t.XXX_Unmarshal(d)
	case proto.Message:
		return proto.Unmarshal(d, t)
	case protobufMsgCodec:
		return t.Unmarshal(d)
	}
	return nil
}

func (c *GrpcCodec) Name() string {
	return "grpc"
}
