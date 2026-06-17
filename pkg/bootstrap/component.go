package bootstrap

import "context"

// closer 把任何带 Close(ctx) error 的对象适配成 lifecycx.Component。
//
// SPEC OQ-003 结论：configx/observex/resiliencx Client 和 7 存储 adapter Client
// 都有 Close(ctx) error 但没有 Start/Name，不实现 lifecycx.Component。
// bootstrap 用 closerComponent wrapper 统一适配：
//   - Start = no-op（Client 在构造时已连接）
//   - Stop  = Close
//   - Name  = 构造时传入
type closerComponent struct {
	name  string
	close func(ctx context.Context) error
}

// newCloserComponent 从 name + closer 构造 Component。
func newCloserComponent(name string, closer func(ctx context.Context) error) *closerComponent {
	return &closerComponent{name: name, close: closer}
}

// Name 返回组件名。
func (c *closerComponent) Name() string { return c.name }

// Start 是 no-op。基座 Client 在 New() 时已完成初始化与连接。
func (c *closerComponent) Start(ctx context.Context) error { return nil }

// Stop 调用底层 Close。
func (c *closerComponent) Stop(ctx context.Context) error {
	if c.close == nil {
		return nil
	}
	return c.close(ctx)
}
