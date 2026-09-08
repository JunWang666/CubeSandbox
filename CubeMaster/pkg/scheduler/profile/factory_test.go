// Copyright (c) 2024 Tencent Inc.
// SPDX-License-Identifier: Apache-2.0
//

package profile

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/base/config"
	"github.com/tencentcloud/CubeSandbox/CubeMaster/pkg/scheduler/selctx"
)

// TestFactoryProfilesCompile 锁死内嵌出厂配置的合法性：用户零调度配置时
// config.Init 经 preHandle 注入三条内置 profile，它们必须能连同真实内置
// scorer 一起编译成功，且路由/兜底符合预期。出厂 YAML 一旦损坏，所有零配置
// 部署都会启动失败，这个测试是最后的安全网。
func TestFactoryProfilesCompile(t *testing.T) {
	dir := t.TempDir()
	confPath := filepath.Join(dir, "conf.yaml")
	content := []byte("common: {}\nlog:\n  module: factory-test\n  path: " + filepath.Join(dir, "log") + "\n")
	if err := os.WriteFile(confPath, content, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CUBE_MASTER_CONFIG_PATH", confPath)
	if _, err := config.Init(); err != nil {
		t.Fatalf("zero-scheduler-config init must succeed: %v", err)
	}

	set, err := Compile(context.Background(), config.GetConfig(), profileRegistry(t))
	if err != nil {
		t.Fatalf("factory profiles must compile: %v", err)
	}
	t.Cleanup(func() { _ = set.Close() })

	wantNames := []string{"burst_balance", "mixed_binpack", "template_reuse"}
	gotNames := set.Names()
	if len(gotNames) != len(wantNames) {
		t.Fatalf("profile names = %v, want %v", gotNames, wantNames)
	}
	for i := range wantNames {
		if gotNames[i] != wantNames[i] {
			t.Fatalf("profile names = %v, want %v", gotNames, wantNames)
		}
	}

	routes := []struct {
		labels map[string]string
		want   string
	}{
		{map[string]string{"workload": "burst_balance"}, "burst_balance"},
		{map[string]string{"workload": "template_reuse"}, "template_reuse"},
		{nil, "mixed_binpack"},
		{map[string]string{"workload": "unlisted"}, "mixed_binpack"},
	}
	for _, route := range routes {
		pipeline := set.Match(&selctx.SelectorCtx{RequestLabels: route.labels})
		if pipeline == nil || pipeline.Name != route.want {
			t.Fatalf("labels %v routed to %+v, want %q", route.labels, pipeline, route.want)
		}
	}
}
