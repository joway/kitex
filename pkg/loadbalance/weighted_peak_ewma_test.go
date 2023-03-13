package loadbalance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/discovery"
)

func TestPeakEWMA_SameLoad(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 10, nil),
		discovery.NewInstance("tcp", "addr3", 10, nil),
	}

	picker := newPeakEWMAPicker(insList, 30)
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

func TestPeakEWMA_Weighted(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 10, nil),
		discovery.NewInstance("tcp", "addr2", 30, nil),
		discovery.NewInstance("tcp", "addr3", 60, nil),
	}

	picker := newPeakEWMAPicker(insList, 100)
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

func TestPeakEWMA_SingleFailure(t *testing.T) {
	ctx := context.Background()
	insList := []discovery.Instance{
		discovery.NewInstance("tcp", "addr1", 1, nil),
		discovery.NewInstance("tcp", "addr2", 2, nil),
		discovery.NewInstance("tcp", "addr3", 3, nil),
		discovery.NewInstance("tcp", "addr4", 3, nil),
	}

	picker := newPeakEWMAPicker(insList, 9)
	var counter1, counter2, counter3, counter4 int32
	var wg sync.WaitGroup
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				ins := picker.Next(ctx, nil)
				addr := ins.Address().String()
				switch addr {
				case "addr1":
					atomic.AddInt32(&counter1, 1)
				case "addr2":
					atomic.AddInt32(&counter2, 1)
				case "addr3":
					atomic.AddInt32(&counter3, 1)
				case "addr4":
					atomic.AddInt32(&counter4, 1)
					time.Sleep(time.Millisecond * 10)
				}
				Release(ins, nil)
			}
		}()
	}
	wg.Wait()
	t.Logf("Access Conter: %d %d %d %d", counter1, counter2, counter3, counter4)
	test.Assert(t, counter1 < counter2)
	test.Assert(t, counter2 < counter3)
	test.Assert(t, counter3 > counter4)
}
