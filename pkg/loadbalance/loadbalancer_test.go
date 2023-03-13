package loadbalance

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal"
	"github.com/cloudwego/kitex/pkg/discovery"
)

var _ discovery.Instance = (*testInstance)(nil)

type testInstanceSetting struct {
	threshold int32         // inflight threshold
	latency   time.Duration // latency if inflight <= threshold
	timeout   time.Duration // latency if inflight > threshold
	jitter    time.Duration // jitter latency for every request
}

func newTestInstance(addr string, weight int, setting testInstanceSetting) *testInstance {
	i := &testInstance{
		setting: setting,
		addr:    addr,
		weight:  weight,
	}
	go i.run()
	return i
}

type testJob struct {
	duration time.Duration
	done     chan struct{}
}

type testInstance struct {
	addr    string
	weight  int
	setting testInstanceSetting
	queue   chan *testJob

	// stats
	totalRequests       int32 // requests has been processed
	inflightRequests    int32 // requests is processing
	maxInflightRequests int32 // max requests is processing
	throttledRequests   int32 // requests that has been throttled
}

func (i *testInstance) Address() net.Addr {
	return &net.UnixAddr{Net: "unix", Name: i.addr}
}

func (i *testInstance) Weight() int {
	return i.weight
}

func (i *testInstance) Tag(key string) (value string, exist bool) {
	// use Tag to do call
	if key == "call" {
		i.Call()
	}
	return
}

func (i *testInstance) run() {
	for t := range i.queue {
		time.Sleep(t.duration)
		close(t.done)
	}
}

func (i *testInstance) Call() {
	total := atomic.AddInt32(&i.totalRequests, 1)
	inflight := atomic.AddInt32(&i.inflightRequests, 1)
	maxInflight := atomic.LoadInt32(&i.maxInflightRequests)
	if inflight > maxInflight {
		atomic.StoreInt32(&i.maxInflightRequests, inflight)
	}
	throttled := false
	if inflight > i.setting.threshold {
		throttled = true
		atomic.AddInt32(&i.throttledRequests, 1)
	}

	latency := i.setting.latency
	if throttled {
		latency = i.setting.timeout
	}
	if total%3 == 1 {
		latency += i.setting.jitter
	} else if total%3 == 2 {
		latency -= i.setting.jitter
	}

	job := &testJob{duration: latency, done: make(chan struct{})}
	i.queue <- job
	<-job.done

	atomic.AddInt32(&i.inflightRequests, -1)
}

func Report(instances []discovery.Instance) {
	fmt.Println("---------------------")
	var maxInflight int32
	var throttled int32
	for i, _ := range instances {
		ins := instances[i].(*testInstance)
		if ins.maxInflightRequests > maxInflight {
			maxInflight = ins.maxInflightRequests
		}
		throttled += ins.throttledRequests
	}
	fmt.Printf("maxInflight=%d, throttledRequest=%d\n", maxInflight, throttled)
}

func TestLoadBalancer(t *testing.T) {
	t.SkipNow()
	ctx := context.Background()
	balancerTestcases := []*balancerTestcase{
		{Name: "weight_least_load", factory: NewWeightedLeastLoadBalancer},
		{Name: "weight_peak_ewma", factory: NewWeightedPeakEWMABalancer},
	}
	for _, tc := range balancerTestcases {
		t.Run(tc.Name, func(t *testing.T) {
			concurrency := 10
			requests := 1000 / concurrency
			instanceChoice := []int{1, 3, 10, 30, 100}
			for _, n := range instanceChoice {
				t.Run(fmt.Sprintf("%d-instances", n), func(t *testing.T) {
					setting := testInstanceSetting{
						threshold: 10,
						latency:   time.Millisecond * 20,
						timeout:   time.Millisecond * 1000,
						jitter:    time.Millisecond,
					}
					normalInstances := makeNTestInstances(n-1, setting)
					setting.jitter = time.Millisecond * 100
					abnormalInstances := makeNTestInstances(1, setting)
					instances := append(normalInstances, abnormalInstances...)
					e := discovery.Result{
						Cacheable: true,
						CacheKey:  "test",
						Instances: instances,
					}
					balancer := tc.factory()
					var wg sync.WaitGroup
					for c := 0; c < concurrency; c++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							for i := 0; i < requests; i++ {
								picker := balancer.GetPicker(e)
								ins := picker.Next(ctx, nil)
								ins.Tag("call")
								Release(ins, nil)
								if r, ok := picker.(internal.Reusable); ok {
									r.Recycle()
								}
							}
						}()
					}
					wg.Wait()
					Report(instances)
				})
			}
		})
	}
}

func makeNTestInstances(n int, setting testInstanceSetting) (res []discovery.Instance) {
	for i := 0; i < n; i++ {
		res = append(res, newTestInstance(fmt.Sprintf("%d", i), 10, setting))
	}
	return res
}
