/*
 * Copyright 2023 CloudWeGo Authors
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

package loadbalance

import (
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/klog"
)

const (
	lbKindRoundRobin = 1
	lbKindRandom     = 2
	lbKindLeastLoad  = 3
	lbKindPeakEWMA   = 4
)

type weightedBalancer struct {
	kind        int
	pickerCache sync.Map
	sfg         singleflight.Group
}

// NewWeightedBalancer creates a loadbalancer using weighted-round-robin algorithm.
func NewWeightedBalancer() Loadbalancer {
	return NewWeightedRoundRobinBalancer()
}

// NewWeightedRoundRobinBalancer creates a loadbalancer using weighted-round-robin algorithm.
func NewWeightedRoundRobinBalancer() Loadbalancer {
	lb := &weightedBalancer{kind: lbKindRoundRobin}
	return lb
}

// NewWeightedRandomBalancer creates a loadbalancer using weighted-random algorithm.
func NewWeightedRandomBalancer() Loadbalancer {
	lb := &weightedBalancer{kind: lbKindRandom}
	return lb
}

// NewWeightedLeastLoadBalancer creates a loadbalancer using weighted-least-load algorithm.
func NewWeightedLeastLoadBalancer() Loadbalancer {
	lb := &weightedBalancer{kind: lbKindLeastLoad}
	return lb
}

// NewWeightedPeakEWMABalancer creates a loadbalancer using weighted-peak-ewma algorithm.
func NewWeightedPeakEWMABalancer() Loadbalancer {
	lb := &weightedBalancer{kind: lbKindPeakEWMA}
	return lb
}

// GetPicker implements the Loadbalancer interface.
func (wb *weightedBalancer) GetPicker(e discovery.Result) Picker {
	if !e.Cacheable {
		picker := wb.createPicker(e)
		return picker
	}

	picker, ok := wb.pickerCache.Load(e.CacheKey)
	if !ok {
		picker, _, _ = wb.sfg.Do(e.CacheKey, func() (interface{}, error) {
			p := wb.createPicker(e)
			wb.pickerCache.Store(e.CacheKey, p)
			return p, nil
		})
	}
	return picker.(Picker)
}

func (wb *weightedBalancer) createPicker(e discovery.Result) (picker Picker) {
	instances := make([]discovery.Instance, 0, len(e.Instances))
	balance := true
	weightSum := 0
	for i := 0; i < len(e.Instances); i++ {
		instance := e.Instances[i]
		weight := instance.Weight()
		if weight <= 0 {
			klog.Warnf("KITEX: invalid weight, weight=%d instance=%s", weight, instance.Address())
			continue
		}
		if balance && i > 0 && weight != e.Instances[i-1].Weight() {
			balance = false
		}
		weightSum += weight
		instances = append(instances, instance)
	}
	if len(instances) == 0 {
		return new(DummyPicker)
	}

	switch wb.kind {
	case lbKindRoundRobin:
		if balance {
			picker = newRoundRobinPicker(instances)
		} else {
			picker = newWeightedRoundRobinPicker(instances)
		}
	case lbKindRandom:
		if balance {
			picker = newRandomPicker(instances)
		} else {
			picker = newWeightedRandomPicker(instances, weightSum)
		}
	case lbKindLeastLoad:
		picker = newWeightedLeastLoadPicker(instances, weightSum)
	case lbKindPeakEWMA:
		picker = newPeakEWMAPicker(instances, weightSum)
	}
	return picker
}

// Rebalance implements the Rebalancer interface.
func (wb *weightedBalancer) Rebalance(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	wb.pickerCache.Store(change.Result.CacheKey, wb.createPicker(change.Result))
}

// Delete implements the Rebalancer interface.
func (wb *weightedBalancer) Delete(change discovery.Change) {
	if !change.Result.Cacheable {
		return
	}
	wb.pickerCache.Delete(change.Result.CacheKey)
}

func (wb *weightedBalancer) Name() string {
	switch wb.kind {
	case lbKindRoundRobin:
		return "weight_round_robin"
	case lbKindRandom:
		return "weight_random"
	case lbKindLeastLoad:
		return "weight_least_load"
	case lbKindPeakEWMA:
		return "weight_peak_ewma"
	}
	return ""
}
