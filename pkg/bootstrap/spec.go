package bootstrap

import (
	"context"
	"errors"

	"github.com/ZoneCNH/configx/pkg/configx"
	"github.com/ZoneCNH/kernel/lifecycx"
	"github.com/ZoneCNH/observex/pkg/observex"
	"github.com/ZoneCNH/resiliencx/pkg/resiliencx"
)

// Spec 描述一个进程的标准组件清单。
type Spec struct {
	// Module 是进程名，如 "binance" / "market-data"。必填。
	Module string
	// Stores 是存储启用的位掩码。adapter 传 None（零存储），聚合层传 All 或组合。
	Stores StoreSet
	// Hooks 是可选的组装回调，在 Build 末尾、Lifecycle.Start 之前调用。
	// 用于注册服务自定义的 Component。
	Hooks []func(*App) error
}

// Validate 校验 Spec 字段合法性。
func (s Spec) Validate() error {
	if s.Module == "" {
		return ErrEmptyModule
	}
	return nil
}

// StoreSet 是存储启用的位掩码。
type StoreSet uint8

// 7 个存储位。
const (
	None StoreSet = 0
	TD   StoreSet = 1 << iota
	PG
	Redis
	Kafka
	NATS
	OSS
	CH
	// All 启用全部 7 个存储（聚合层使用）。
	All StoreSet = TD | PG | Redis | Kafka | NATS | OSS | CH
)

// Has 报告 s 是否启用指定存储位。
func (s StoreSet) Has(bit StoreSet) bool { return s&bit != 0 }

// String 返回 StoreSet 的人类可读表示（如 "TD|PG|Redis"）。
func (s StoreSet) String() string {
	if s == None {
		return "None"
	}
	if s == All {
		return "All"
	}
	var parts []string
	if s.Has(TD) {
		parts = append(parts, "TD")
	}
	if s.Has(PG) {
		parts = append(parts, "PG")
	}
	if s.Has(Redis) {
		parts = append(parts, "Redis")
	}
	if s.Has(Kafka) {
		parts = append(parts, "Kafka")
	}
	if s.Has(NATS) {
		parts = append(parts, "NATS")
	}
	if s.Has(OSS) {
		parts = append(parts, "OSS")
	}
	if s.Has(CH) {
		parts = append(parts, "CH")
	}
	return join(parts, "|")
}

// App 是组装后的运行时句柄。
//
// 注意：Config/Observe/Resilience 的 Client 均无业务 getter（只有 Close/HealthCheck），
// 这些字段仅供 Shutdown 时统一 Close，不供服务取 logger/metrics。
// 服务要可观测，自行 observex.New。
type App struct {
	Config     *configx.Client    // 仅供 Close
	Observe    *observex.Client   // 仅供 Close
	Resilience *resiliencx.Client // 仅供 Close
	Lifecycle  *lifecycx.Manager  // 统一 Start/Stop 编排
	ConfigHash string             // configx EffectiveConfigHash（启动日志用）
	started    bool
}

// Stores 持有启用的存储适配器 Client 句柄。
// 未启用的位为 nil。Stores == nil 表示零存储（adapter）。
//
// 各字段为 interface{} 类型，实际为对应 adapter 的 *Client。
// 这是 SPEC v0.1.1 OQ-003 的结论：存储适配器未实现 lifecycx.Component，
// bootstrap 用 closerComponent wrapper 适配其 Close 方法。
type Stores struct {
	TD    any // *taosx.Client
	PG    any // *postgresx.Client
	Redis any // *redisx.Client
	Kafka any // *kafkax.Client
	NATS  any // *natsx.Client
	OSS   any // *ossx.Client
	CH    any // *clickhousex.Client
}

// 错误定义。

// ErrEmptyModule 当 Spec.Module 为空时返回。
var ErrEmptyModule = errors.New("bootstrap: Spec.Module is empty")

// ErrBuildFailed 当 Build 过程中某步骤失败时返回（包装底层错误）。
var ErrBuildFailed = errors.New("bootstrap: build failed")

// ErrNotStarted 当 Run/Shutdown 在 Start 前调用时返回。
var ErrNotStarted = errors.New("bootstrap: app not started")

// join 是 strings.Join 的本地拷贝，避免引入额外 import（保持最小依赖）。
func join(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}

// ensure context is referenced（spec.go 保留 context import 供未来 Hooks 签名使用）
var _ = context.Background
