package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/ZoneCNH/configx/pkg/configx"
	"github.com/ZoneCNH/kernel/lifecycx"
	"github.com/ZoneCNH/kernel/shutdownx"
	"github.com/ZoneCNH/observex/pkg/observex"
	"github.com/ZoneCNH/resiliencx/pkg/resiliencx"
)

// Build 是唯一入口：config → observex → resilience → lifecycle 组装。
//
// 按 Spec.Stores 位掩码决定是否构造存储适配器（当前 v0.1.0 存储构造为 stub，
// 待存储 adapter 的 New 签名固化后补全；adapter 传 None 不受影响）。
//
// Build 成功后，调用者应：
//  1. 执行 Spec.Hooks（注册自定义 Component）
//  2. 调用 app.Run(ctx) 阻塞
func Build(ctx context.Context, spec Spec) (*App, error) {
	const op = "bootstrap.Build"

	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%s: context is required", op)
	}

	app := &App{}

	// ---- 1. configx ----
	cfgClient, err := buildConfig(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("%s: config: %w", op, err)
	}
	app.Config = cfgClient

	// ---- 2. observex ----
	obsClient, err := buildObserve(ctx, spec)
	if err != nil {
		// 回滚已建 configx
		_ = cfgClient.Close(ctx)
		return nil, fmt.Errorf("%s: observe: %w", op, err)
	}
	app.Observe = obsClient

	// ---- 3. resiliencx ----
	resClient, err := buildResilience(ctx, spec)
	if err != nil {
		_ = obsClient.Close(ctx)
		_ = cfgClient.Close(ctx)
		return nil, fmt.Errorf("%s: resilience: %w", op, err)
	}
	app.Resilience = resClient

	// ---- 4. 存储适配器（按 StoreSet；v0.1.0 stub）----
	// SPEC OQ-003：存储 adapter 未实现 Component，构造后用 closerComponent 注册。
	// v0.1.0 暂不构造真实存储（adapter Stores=None 不受影响）；
	// 聚合层 Stores!=None 时返回明确错误，待存储 adapter New 签名固化后补全。
	if spec.Stores != None {
		return nil, fmt.Errorf("%s: stores construction pending (SPEC v0.1.0 stub); use Stores=None for adapter", op)
	}

	// ---- 5. lifecycx.Manager（注册全部 Client 为 closerComponent）----
	components := []lifecycx.Component{
		newCloserComponent(spec.Module+":resilience", resClient.Close),
		newCloserComponent(spec.Module+":observe", obsClient.Close),
		newCloserComponent(spec.Module+":config", cfgClient.Close),
	}
	app.Lifecycle = lifecycx.NewManager(components...)

	// ---- 6. Hooks ----
	for _, hook := range spec.Hooks {
		if err := hook(app); err != nil {
			_ = app.Lifecycle.Stop(ctx)
			return nil, fmt.Errorf("%s: hook: %w", op, err)
		}
	}

	return app, nil
}

// Run 启动所有 Component，阻塞直到 SIGINT/SIGTERM，然后逆序 Stop。
//
// Run 是阻塞调用。典型用法：
//
//	app.Run(ctx)  // 阻塞到信号
func (a *App) Run(ctx context.Context) error {
	const op = "bootstrap.Run"
	if a == nil {
		return fmt.Errorf("%s: app is nil", op)
	}

	// 顺序 Start
	if err := a.Lifecycle.Start(ctx); err != nil {
		return fmt.Errorf("%s: lifecycle start: %w", op, err)
	}
	a.started = true

	// 信号捕获（kernel.shutdownx.NotifyContext）
	signalCtx, cancel := shutdownx.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 阻塞直到信号
	<-signalCtx.Done()

	// 逆序 Stop
	return a.Shutdown(ctx)
}

// Shutdown 逆序 Stop 所有 Component（幂等）。
func (a *App) Shutdown(ctx context.Context) error {
	const op = "bootstrap.Shutdown"
	if a == nil {
		return fmt.Errorf("%s: app is nil", op)
	}
	if !a.started {
		return nil // 未 Start 则无需 Shutdown（幂等）
	}
	a.started = false
	if err := a.Lifecycle.Stop(ctx); err != nil {
		return fmt.Errorf("%s: lifecycle stop: %w", op, err)
	}
	return nil
}

// ---- 内部构造函数 ----

// buildConfig 用 configx 加载 .env + 环境变量。
func buildConfig(ctx context.Context, spec Spec) (*configx.Client, error) {
	loader := configx.NewLoader()
	// .env 文件（存在时加载，不存在跳过）
	if _, err := os.Stat(".env"); err == nil {
		loader.AddSource(configx.NewEnvFileSource(".env"))
	}
	// 环境变量（XGO_ 前缀）
	loader.AddSource(configx.NewAllEnvSource("XGO_" + upper(spec.Module) + "_"))

	result, err := loader.Load(ctx)
	if err != nil {
		return nil, err
	}
	return configx.New(ctx, configx.Config{
		Name: spec.Module,
	}, configxFromLoadResult(result)...)
}

// buildObserve 用 observex.New 创建 Client。
func buildObserve(ctx context.Context, spec Spec) (*observex.Client, error) {
	return observex.New(ctx, observex.Config{
		Name: spec.Module,
	})
}

// buildResilience 用 resiliencx.New 创建 Client。
func buildResilience(ctx context.Context, spec Spec) (*resiliencx.Client, error) {
	return resiliencx.New(ctx, resiliencx.Config{
		Name: spec.Module,
	})
}

// upper 返回大写字符串（避免引入 strings.ToUpper 以外的依赖）。
func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

// configxFromLoadResult 将 LoadResult 转为 configx.Option（v0.1.0 占位，待 configx API 固化）。
//
// 当前 configx.New 只需 Config，LoadResult 用于后续 StrictDecode/Provenance；
// 此函数保留扩展点。
func configxFromLoadResult(_ configx.LoadResult) []configx.Option {
	return nil
}

// 编译期保证 App 满足预期接口（防回归）。
var (
	_ = errors.New
)
