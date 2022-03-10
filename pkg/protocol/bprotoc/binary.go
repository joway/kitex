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

package bprotoc

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"math"
)

// Binary protocol for bthrift.
var Binary binaryProtocol

// When encoding length-prefixed fields, we speculatively set aside some number of bytes
// for the length, encode the data, and then encode the length (shifting the data if necessary
// to make room).
const speculativeLength = 1

var _ BProtocol = binaryProtocol{}

type binaryProtocol struct{}

// *struct 应该没有 packed(只写 len+val); 因此只需要实现 tag+len+val 和 val 两种;
// 此处不同于其他类型, 其他类型均实现了 tag+len+val 和 len+val 两种
func (b binaryProtocol) WriteMessage(buf []byte, number int32, fastWrite FastWrite) (n int) {
	// 这里如果 number < 0; 则只写 val
	if number == SkipTagNumber {
		return fastWrite.FastWrite(buf)
	}
	// tag+len+val
	n += AppendTag(buf, protowire.Number(number), protowire.BytesType)
	buf = buf[n:]

	// AppendSpeculativeLength
	prefix := speculativeLength
	mlen := fastWrite.FastWrite(buf[prefix:])

	// FinishSpeculativeLength
	msiz := protowire.SizeVarint(uint64(mlen))
	if msiz != speculativeLength {
		copy(buf[msiz:], buf[prefix:prefix+mlen])
	}
	AppendVarint(buf[:msiz], uint64(mlen))
	n += msiz + mlen
	return n
}

// only tag+len+val
func (b binaryProtocol) WriteListPacked(buf []byte, number int32, length int, single BMarshal) (n int) {
	n += AppendTag(buf, protowire.Number(number), protowire.BytesType)
	buf = buf[n:]

	// AppendSpeculativeLength
	prefix := speculativeLength
	offset := prefix
	for i := 0; i < length; i++ {
		offset += single(buf[offset:], int32(SkipTagNumber), int32(i))
	}
	mlen := offset - prefix

	// FinishSpeculativeLength
	msiz := protowire.SizeVarint(uint64(mlen))
	if msiz != speculativeLength {
		copy(buf[msiz:], buf[prefix:prefix+mlen])
	}
	AppendVarint(buf[:msiz], uint64(mlen))

	n += msiz + mlen
	return n
}

// only tag+len+val
func (b binaryProtocol) WriteMapEntry(buf []byte, number int32, entry BMarshal) (n int) {
	n += AppendTag(buf, protowire.Number(number), protowire.BytesType)
	buf = buf[n:]

	// AppendSpeculativeLength
	prefix := speculativeLength
	mlen := entry(buf[prefix:], MapEntry_Key_field_number, MapEntry_Value_field_number)

	// FinishSpeculativeLength
	msiz := protowire.SizeVarint(uint64(mlen))
	if msiz != speculativeLength {
		copy(buf[msiz:], buf[prefix:prefix+mlen])
	}
	AppendVarint(buf[:msiz], uint64(mlen))

	n += msiz + mlen
	return n
}

func (b binaryProtocol) WriteBool(buf []byte, number int32, value bool) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.BoolKind])
	}
	n += AppendVarint(buf[n:], protowire.EncodeBool(value))
	return n
}

func (b binaryProtocol) WriteInt32(buf []byte, number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Int32Kind])
	}
	n += AppendVarint(buf[n:], uint64(value))
	return n
}

func (b binaryProtocol) WriteInt64(buf []byte, number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Int64Kind])
	}
	n += AppendVarint(buf[n:], uint64(value))
	return n
}

func (b binaryProtocol) WriteUint32(buf []byte, number int32, value uint32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Uint32Kind])
	}
	n += AppendVarint(buf[n:], uint64(value))
	return n
}

func (b binaryProtocol) WriteUint64(buf []byte, number int32, value uint64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Uint64Kind])
	}
	n += AppendVarint(buf[n:], value)
	return n
}

func (b binaryProtocol) WriteSint32(buf []byte, number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Sint32Kind])
	}
	n += AppendVarint(buf[n:], protowire.EncodeZigZag(int64(value)))
	return n
}

func (b binaryProtocol) WriteSint64(buf []byte, number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Sint64Kind])
	}
	n += AppendVarint(buf[n:], protowire.EncodeZigZag(value))
	return n
}

func (b binaryProtocol) WriteFloat(buf []byte, number int32, value float32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.FloatKind])
	}
	n += AppendFixed32(buf[n:], math.Float32bits(value))
	return n
}

func (b binaryProtocol) WriteDouble(buf []byte, number int32, value float64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.DoubleKind])
	}
	n += AppendFixed64(buf[n:], math.Float64bits(value))
	return n
}

func (b binaryProtocol) WriteFixed32(buf []byte, number int32, value uint32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Fixed32Kind])
	}
	n += AppendFixed32(buf[n:], value)
	return n
}

func (b binaryProtocol) WriteFixed64(buf []byte, number int32, value uint64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Fixed64Kind])
	}
	n += AppendFixed64(buf[n:], value)
	return n
}

func (b binaryProtocol) WriteSfixed32(buf []byte, number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Sfixed32Kind])
	}
	n += AppendFixed32(buf[n:], uint32(value))
	return n
}

func (b binaryProtocol) WriteSfixed64(buf []byte, number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.Sfixed64Kind])
	}
	n += AppendFixed64(buf[n:], uint64(value))
	return n
}

func (b binaryProtocol) WriteString(buf []byte, number int32, value string) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.StringKind])
	}
	// only support proto3
	//if EnforceUTF8() && !utf8.ValidString(value) {
	//	panic(errInvalidUTF8)
	//}
	n += AppendString(buf[n:], value)
	return n
}

func (b binaryProtocol) WriteBytes(buf []byte, number int32, value []byte) (n int) {
	if number != SkipTagNumber {
		n += AppendTag(buf[n:], protowire.Number(number), wireTypes[protoreflect.BytesKind])
	}
	n += AppendBytes(buf[n:], value)
	return n
}

func (b binaryProtocol) ReadBool(buf []byte, _type int8) (value bool, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}

	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return protowire.DecodeBool(v), n, nil
}

func (b binaryProtocol) ReadInt32(buf []byte, _type int8) (value int32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return int32(v), n, nil
}

func (b binaryProtocol) ReadInt64(buf []byte, _type int8) (value int64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return int64(v), n, nil
}

func (b binaryProtocol) ReadUint32(buf []byte, _type int8) (value uint32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return uint32(v), n, nil
}

func (b binaryProtocol) ReadUint64(buf []byte, _type int8) (value uint64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return v, n, nil
}

func (b binaryProtocol) ReadSint32(buf []byte, _type int8) (value int32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return int32(protowire.DecodeZigZag(v & math.MaxUint32)), n, nil
}

func (b binaryProtocol) ReadSint64(buf []byte, _type int8) (value int64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.VarintType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeVarint(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return protowire.DecodeZigZag(v), n, nil
}

func (b binaryProtocol) ReadFloat(buf []byte, _type int8) (value float32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed32Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed32(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return math.Float32frombits(uint32(v)), n, nil
}

func (b binaryProtocol) ReadDouble(buf []byte, _type int8) (value float64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed64Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed64(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return math.Float64frombits(v), n, nil
}

func (b binaryProtocol) ReadFixed32(buf []byte, _type int8) (value uint32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed32Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed32(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return uint32(v), n, nil
}

func (b binaryProtocol) ReadFixed64(buf []byte, _type int8) (value uint64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed64Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed64(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return v, n, nil
}

func (b binaryProtocol) ReadSfixed32(buf []byte, _type int8) (value int32, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed32Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed32(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return int32(v), n, nil
}

func (b binaryProtocol) ReadSfixed64(buf []byte, _type int8) (value int64, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.Fixed64Type {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeFixed64(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	return int64(v), n, nil
}

func (b binaryProtocol) ReadString(buf []byte, _type int8) (value string, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != protowire.BytesType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeBytes(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	// only support proto3
	//if EnforceUTF8() && !utf8.Valid(v) {
	//	return value, 0, errInvalidUTF8
	//}
	return string(v), n, nil
}

func (b binaryProtocol) ReadBytes(buf []byte, _type int8) (value []byte, n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != protowire.BytesType {
		return value, 0, errUnknown
	}
	v, n := protowire.ConsumeBytes(buf)
	if n < 0 {
		return value, 0, errDecode
	}
	value = make([]byte, len(v))
	copy(value, v)
	return value, n, nil
}

func (b binaryProtocol) ReadList(buf []byte, _type int8, single BUnmarshal) (n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp == protowire.BytesType {
		var framed []byte
		framed, n = protowire.ConsumeBytes(buf)
		if n < 0 {
			return 0, errDecode
		}
		for len(framed) > 0 {
			off, err := single(buf, int8(SkipTypeCheck))
			if err != nil {
				return 0, err
			}
			framed = framed[off:]
		}
		return n, nil
	}
	n, err = single(buf, _type)
	return n, err
}

func (b binaryProtocol) ReadMapEntry(buf []byte, _type int8, umk, umv BUnmarshal) (int, error) {
	offset := 0
	wtyp := protowire.Type(_type)
	if wtyp != protowire.BytesType {
		return 0, errUnknown
	}
	bs, n := protowire.ConsumeBytes(buf)
	if n < 0 {
		return 0, errDecode
	}
	offset = n
	// Map entries are represented as a two-element message with fields
	// containing the key and value.
	for len(bs) > 0 {
		num, wtyp, n := protowire.ConsumeTag(bs)
		if n < 0 {
			return 0, errDecode
		}
		if num > protowire.MaxValidNumber {
			return 0, errDecode
		}
		bs = bs[n:]
		err := errUnknown
		switch num {
		case MapEntry_Key_field_number:
			n, err = umk(bs, int8(wtyp))
			if err != nil {
				break
			}
			//haveKey = true
		case MapEntry_Value_field_number:
			n, err = umv(bs, int8(wtyp))
			if err != nil {
				break
			}
			//haveVal = true
		}
		if err == errUnknown {
			n, err = b.Skip(buf, int8(wtyp), int32(num))
			if err != nil {
				return 0, err
			}
		}
		bs = bs[n:]
	}
	return offset, nil
}

func (b binaryProtocol) ReadMessage(buf []byte, _type int8, fastRead FastRead) (n int, err error) {
	wtyp := protowire.Type(_type)
	if wtyp != SkipTypeCheck && wtyp != protowire.BytesType {
		return 0, errUnknown
	}
	// framed
	if wtyp == protowire.BytesType {
		buf, n = protowire.ConsumeBytes(buf)
		if n < 0 {
			return 0, errDecode
		}
	}
	offset := 0
	for offset < len(buf) {
		// Parse the tag (field number and wire type).
		num, wtyp, l := protowire.ConsumeTag(buf[offset:])
		offset += l
		if l < 0 {
			return offset, errDecode
		}
		if num > protowire.MaxValidNumber {
			return offset, errDecode
		}
		l, err = fastRead.FastRead(buf[offset:], int8(wtyp), int32(num))
		if err != nil {
			return offset, err
		}
		offset += l
	}
	// check if framed
	if n == 0 {
		n = offset
	}
	return n, nil
}

func (b binaryProtocol) Skip(buf []byte, _type int8, number int32) (n int, err error) {
	n = protowire.ConsumeFieldValue(protowire.Number(number), protowire.Type(_type), buf)
	if n < 0 {
		return 0, errDecode
	}
	return n, nil
}

func (b binaryProtocol) SizeBool(number int32, value bool) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.BoolKind]))
	}
	n += protowire.SizeVarint(protowire.EncodeBool(value))
	return n
}

func (b binaryProtocol) SizeInt32(number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Int32Kind]))
	}
	n += protowire.SizeVarint(uint64(value))
	return n
}

func (b binaryProtocol) SizeInt64(number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Int64Kind]))
	}
	n += protowire.SizeVarint(uint64(value))
	return n
}

func (b binaryProtocol) SizeUint32(number int32, value uint32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Uint32Kind]))
	}
	n += protowire.SizeVarint(uint64(value))
	return n
}

func (b binaryProtocol) SizeUint64(number int32, value uint64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Uint64Kind]))
	}
	n += protowire.SizeVarint(value)
	return n
}

func (b binaryProtocol) SizeSint32(number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Sint32Kind]))
	}
	n += protowire.SizeVarint(protowire.EncodeZigZag(int64(value)))
	return n
}

func (b binaryProtocol) SizeSint64(number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Sint64Kind]))
	}
	n += protowire.SizeVarint(protowire.EncodeZigZag(value))
	return n
}

func (b binaryProtocol) SizeFloat(number int32, value float32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.FloatKind]))
	}
	n += protowire.SizeFixed32()
	return n
}

func (b binaryProtocol) SizeDouble(number int32, value float64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.DoubleKind]))
	}
	n += protowire.SizeFixed64()
	return n
}

func (b binaryProtocol) SizeFixed32(number int32, value uint32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Fixed32Kind]))
	}
	n += protowire.SizeFixed32()
	return n
}

func (b binaryProtocol) SizeFixed64(number int32, value uint64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Fixed64Kind]))
	}
	n += protowire.SizeFixed64()
	return n
}

func (b binaryProtocol) SizeSfixed32(number int32, value int32) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Sfixed32Kind]))
	}
	n += protowire.SizeFixed32()
	return n
}

func (b binaryProtocol) SizeSfixed64(number int32, value int64) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.Sfixed64Kind]))
	}
	n += protowire.SizeFixed64()
	return n
}

func (b binaryProtocol) SizeString(number int32, value string) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.StringKind]))
	}
	n += protowire.SizeBytes(len(value))
	return n
}

func (b binaryProtocol) SizeBytes(number int32, value []byte) (n int) {
	if number != SkipTagNumber {
		n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.BytesKind]))
	}
	n += protowire.SizeBytes(len(value))
	return n
}

func (b binaryProtocol) SizeMessage(number int32, size FastSize) (n int) {
	// 这里如果 number < 0; 则只写 val
	if number == SkipTagNumber {
		return size.Size()
	}
	// tag+len+val
	n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.BytesKind]))
	n += protowire.SizeBytes(size.Size())
	return n
}

func (b binaryProtocol) SizeListPacked(number int32, length int, single BSize) (n int) {
	n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.BytesKind]))
	// append mlen
	mlen := 0
	for i := 0; i < length; i++ {
		mlen += single(int32(SkipTagNumber), int32(i))
	}
	n += protowire.SizeBytes(mlen)
	return n
}

func (b binaryProtocol) SizeMapEntry(number int32, entry BSize) (n int) {
	n += protowire.SizeVarint(protowire.EncodeTag(protowire.Number(number), wireTypes[protoreflect.BytesKind]))
	mlen := entry(MapEntry_Key_field_number, MapEntry_Value_field_number)
	n += protowire.SizeBytes(mlen)
	return n
}
