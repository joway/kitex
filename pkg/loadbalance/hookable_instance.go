package loadbalance

import (
	"sync"

	"github.com/cloudwego/kitex/pkg/discovery"
)

var hookableInstancePool sync.Pool

func NewHookableInstance(ins discovery.Instance, callback func(err error)) *hookableInstance {
	var result *hookableInstance
	p := hookableInstancePool.Get()
	if p == nil {
		result = new(hookableInstance)
	} else {
		result = p.(*hookableInstance)
	}
	result.Instance = ins
	result.callback = callback
	return result
}

type hookableInstance struct {
	discovery.Instance
	callback func(err error)
}

// Release implements the PickResult interface.
func (pr *hookableInstance) Release(err error) {
	if pr.callback != nil {
		pr.callback(err)
	}
	pr.Instance = nil
	pr.callback = nil
	hookableInstancePool.Put(pr)
}
