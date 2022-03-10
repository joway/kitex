package bprotoc

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// [fa0f00]
func TestAppendTag(t *testing.T) {
	buf := make([]byte, 16)
	n := AppendTag(buf, protowire.Number(255), protowire.BytesType)
	gots := fmt.Sprintf("%x", buf[:n])
	if n != 2 || gots != "fa0f" {
		err := fmt.Errorf("AppendTag[%d][%x] != [2][fa0f00]\n", n, buf[:n])
		panic(err)
	}
}

// [fa0f00]
func TestBytes(t *testing.T) {
	num := int32(255)
	value := make([]byte, 0)

	size := Binary.SizeBytes(num, value)
	if size != 3 {
		err := fmt.Errorf("SizeBytes[%d] != 3\n", size)
		panic(err)
	}

	buf := make([]byte, 16)
	wn := Binary.WriteBytes(buf, num, value)
	gots := fmt.Sprintf("%x", buf[:wn])
	if wn != size || gots != "fa0f00" {
		err := fmt.Errorf("WriteBytes[%d][%x] != [%d][fa0f00]\n",
			wn, buf[:wn], size)
		panic(err)
	}
}
