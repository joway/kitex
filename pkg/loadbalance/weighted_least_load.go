package loadbalance

import (
	"context"
	"sync/atomic"

	"github.com/bytedance/gopkg/lang/fastrand"

	"github.com/cloudwego/kitex/pkg/discovery"
)

type weightedLeastLoadPicker struct {
	instances []discovery.Instance
	inflights []int32
	weightSum int
}

func newWeightedLeastLoadPicker(instances []discovery.Instance, weightSum int) Picker {
	return &weightedLeastLoadPicker{
		instances: instances,
		inflights: make([]int32, len(instances)),
		weightSum: weightSum,
	}
}

// Next implements the Picker interface.
func (p *weightedLeastLoadPicker) Next(ctx context.Context, request interface{}) discovery.Instance {
	if len(p.instances) == 0 {
		return nil
	} else if len(p.instances) == 1 {
		return NewHookableInstance(p.instances[0], nil)
	}
	idxA := p.pick()
	idxB := p.pick()
	if idxA < 0 || idxB < 0 {
		return NewHookableInstance(p.instances[0], nil)
	}
	iftA := atomic.LoadInt32(&p.inflights[idxA])
	iftB := atomic.LoadInt32(&p.inflights[idxB])
	var idx int
	if iftA <= iftB {
		idx = idxA
	} else {
		idx = idxB
	}
	atomic.AddInt32(&p.inflights[idx], 1)
	return NewHookableInstance(p.instances[idx], func(_ error) {
		atomic.AddInt32(&p.inflights[idx], -1)
	})
}

func (p *weightedLeastLoadPicker) pick() (idx int) {
	weight := fastrand.Intn(p.weightSum)
	for i := 0; i < len(p.instances); i++ {
		weight -= p.instances[i].Weight()
		if weight < 0 {
			return i
		}
	}
	return -1
}
