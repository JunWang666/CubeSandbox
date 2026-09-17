// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0

package sandbox

import (
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/node"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/localcache"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
)

// trackTemplateCreate records an in-flight template create for local metrics.
func trackTemplateCreate(host *node.Node, selCtx *selctx.SelectorCtx) func() {
	if host == nil || selCtx == nil || selCtx.ReqRes == nil || selCtx.ReqRes.TemplateID == "" {
		return func() {}
	}
	templateID := selCtx.ReqRes.TemplateID
	localcache.IncrNodeTemplateCreate(host.ID(), templateID)
	return func() { localcache.DecrNodeTemplateCreate(host.ID(), templateID) }
}
