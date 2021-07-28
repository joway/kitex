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

package gofunc

import (
	"context"
	"fmt"
	"github.com/bytedance/gopkg/util/logger"
	"runtime/debug"

	xsync "github.com/m3db/m3/src/x/sync"
)

// GoTask is used to spawn a new task.
type GoTask func(context.Context, func())

// GoFunc is the default func used globally.
var GoFunc GoTask
var gopool *xsync.PooledWorkerPool

func init() {
	opts := xsync.NewPooledWorkerPoolOptions()
	opts.SetGrowOnDemand(true)
	gopool, _ := xsync.NewPooledWorkerPool(1024, opts)
	gopool.Init()

	GoFunc = func(ctx context.Context, f func()) {
		gopool.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", "m3-gopool", r, debug.Stack())
					logger.CtxErrorf(ctx, msg)
				}
			}()
			f()
		})
	}
}
