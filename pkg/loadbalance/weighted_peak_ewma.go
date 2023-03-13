package loadbalance

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/gopkg/lang/fastrand"

	"github.com/cloudwego/kitex/pkg/discovery"
)

const (
	decayTime float32 = 1000 // The window of latency observations, ms
)

var ewmaInstancePool sync.Pool

type ewmaInstance struct {
	discovery.Instance
	callback func(start time.Time)
	start    time.Time
}

func newEWMAPickResult(ins discovery.Instance, callback func(start time.Time)) (pr *ewmaInstance) {
	p := ewmaInstancePool.Get()
	if p == nil {
		pr = new(ewmaInstance)
	} else {
		pr = p.(*ewmaInstance)
	}
	pr.Instance = ins
	pr.callback = callback
	pr.start = time.Now()
	return pr
}

func (pr *ewmaInstance) Release(err error) {
	if pr.callback != nil {
		pr.callback(pr.start)
	}
	pr.Instance = nil
	pr.callback = nil
	ewmaInstancePool.Put(pr)
}

// peakEWMAPicker implement peak-EWMA algorithm
// refer: https://github.com/wenkeyang/finagle/blob/53cf698eb9b89e95c06f6b8d8dd0febc43f9142c/finagle-core/src/main/scala/com/twitter/finagle/loadbalancer/P2CBalancer.scala#L73
type peakEWMAPicker struct {
	// read only
	instances []discovery.Instance
	weightSum int
	// atomic values
	ewmaValues []uint64
	timestamp  uint64 // the timestamp of the latest request time in ms
}

func newPeakEWMAPicker(instances []discovery.Instance, weightSum int) Picker {
	ewmaValues := make([]uint64, len(instances))
	return &peakEWMAPicker{
		instances:  instances,
		ewmaValues: ewmaValues,
		weightSum:  weightSum,
	}
}

// Next implements the Picker interface.
func (p *peakEWMAPicker) Next(ctx context.Context, request interface{}) discovery.Instance {
	if len(p.instances) == 0 {
		return nil
	} else if len(p.instances) == 1 {
		return NewHookableInstance(p.instances[0], nil)
	}
	var idx int
	idxA := p.pick()
	idxB := p.pick()
	ewmaA := atomic.LoadUint64(&p.ewmaValues[idxA])
	ewmaB := atomic.LoadUint64(&p.ewmaValues[idxB])
	if ewmaA <= ewmaB {
		idx = idxA
	} else {
		idx = idxB
	}
	ewmaValues := p.ewmaValues
	return newEWMAPickResult(p.instances[idx], func(start time.Time) {
		now := time.Now()
		latency := uint64(now.Sub(start).Milliseconds()) // ms
		if latency < 0 {
			return
		}
		ewma := atomic.LoadUint64(&ewmaValues[idx])
		var newEWMA uint64
		if ewma == 0 {
			// use latency as first ewma value
			newEWMA = latency
		} else if latency > ewma {
			// use peak latency
			newEWMA = latency
		} else {
			// use new ewma value
			current := uint64(now.UnixMilli())
			latest := atomic.SwapUint64(&p.timestamp, current)
			delta := current - latest
			if delta <= 0 {
				return // seems timestamp has been changed by another request
			}
			// Vt = β×Vt−1 + (1−β)×θt, θt is current latency, β is constant, Vt−1 is last ewma value
			//   Vt will be more depend on current latency if β is smaller
			// β = 1/e^(k*Δt), k is constant, Δt is interval between two requests
			//   0 < β <= 1, and decreasing quickly when (k*Δt) is increasing
			//   β=1/e^(k*Δt) => e^(-Δt*k) => e^(-x), x=delta/decay, decay is constant
			//   β is decreasing when delta is increasing
			beta := math.Exp(float64(-delta) / float64(decayTime))
			newEWMA = uint64(beta*float64(ewma) + (1-beta)*float64(latency)) //
		}
		atomic.StoreUint64(&ewmaValues[idx], newEWMA)
	})
}

func (p *peakEWMAPicker) pick() (idx int) {
	weight := fastrand.Intn(p.weightSum)
	for i := 0; i < len(p.instances); i++ {
		weight -= p.instances[i].Weight()
		if weight < 0 {
			return i
		}
	}
	return -1
}
