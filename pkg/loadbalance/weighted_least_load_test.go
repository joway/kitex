package loadbalance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/discovery"
)

func TestLeastLoad_SameLoad(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 10, nil),
		discovery.NewInstance("tcp", "addr3", 10, nil),
	}

	picker := newWeightedLeastLoadPicker(insList, 30)
	accessMap := map[string]int{}
	for i := 0; i < 100000; i++ {
		ins := picker.Next(ctx, nil)
		accessMap[ins.Address().String()]++
		Release(ins, nil)
	}
	t.Logf("Access Map: %v", accessMap)
	test.Assert(t, accessMap["addr2"]/10000 == 3)
	test.Assert(t, accessMap["addr2"]/10000 == 3)
	test.Assert(t, accessMap["addr2"]/10000 == 3)
}

func TestLeastLoad_HighLoad(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 10, nil),
		discovery.NewInstance("tcp", "addr3", 10, nil),
	}

	picker := newWeightedLeastLoadPicker(insList, 30)
	accessMap := map[string]int{}
	for i := 0; i < 100000; i++ {
		ins := picker.Next(ctx, nil)
		accessMap[ins.Address().String()]++
		if ins.Address().String() != "addr1" {
			Release(ins, nil)
		}
	}
	t.Logf("Access Map: %v", accessMap)
	test.Assert(t, accessMap["addr1"]/10000 == 1)
	test.Assert(t, accessMap["addr2"]/10000 == 4)
	test.Assert(t, accessMap["addr3"]/10000 == 4)
}

func TestLeastLoad_Weighted(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 30, nil),
		discovery.NewInstance("tcp", "addr3", 60, nil),
	}

	picker := newWeightedLeastLoadPicker(insList, 100)
	accessMap := map[string]int{}
	for i := 0; i < 100000; i++ {
		ins := picker.Next(ctx, nil)
		accessMap[ins.Address().String()]++
		Release(ins, nil)
	}
	t.Logf("Access Map: %v", accessMap)
	test.Assert(t, (accessMap["addr1"]+1000)/10000 == 1)
	test.Assert(t, (accessMap["addr2"]+1000)/10000 == 3)
	test.Assert(t, (accessMap["addr3"]+1000)/10000 == 6)
}

func TestLeastLoad_Complex(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 30, nil),
		discovery.NewInstance("tcp", "addr3", 60, nil),
	}

	picker := newWeightedLeastLoadPicker(insList, 100)
	var counter1, counter2, counter3 int32
	var wg sync.WaitGroup
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ins := picker.Next(ctx, nil)
			addr := ins.Address().String()
			switch addr {
			case "addr1":
				atomic.AddInt32(&counter1, 1)
				Release(ins, nil)
			case "addr2":
				atomic.AddInt32(&counter2, 1)
				Release(ins, nil)
			case "addr3":
				atomic.AddInt32(&counter3, 1)
				// never return
			}
		}()
	}
	wg.Wait()
	t.Logf("Access Conter: %d %d %d", counter1, counter2, counter3)
	test.Assert(t, counter1 < counter2)
	test.Assert(t, counter2 > counter3)
}
