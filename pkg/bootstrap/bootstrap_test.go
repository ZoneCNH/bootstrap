package bootstrap

import (
	"context"
	"testing"
)

// TestBuildAdapter 验证 Stores=None 时 Build 成功，App.Stores 相关字段为零值。
func TestBuildAdapter(t *testing.T) {
	app, err := Build(context.Background(), Spec{
		Module: "test-adapter",
		Stores: None,
	})
	if err != nil {
		t.Fatalf("Build adapter failed: %v", err)
	}
	defer app.Shutdown(context.Background())

	if app.Config == nil {
		t.Error("app.Config is nil")
	}
	if app.Observe == nil {
		t.Error("app.Observe is nil")
	}
	if app.Resilience == nil {
		t.Error("app.Resilience is nil")
	}
	if app.Lifecycle == nil {
		t.Error("app.Lifecycle is nil")
	}
}

// TestBuildEmptyModule 验证 Spec.Module 为空返回 ErrEmptyModule。
func TestBuildEmptyModule(t *testing.T) {
	_, err := Build(context.Background(), Spec{
		Module: "",
		Stores: None,
	})
	if err == nil {
		t.Fatal("expected ErrEmptyModule, got nil")
	}
	if !errorsIs(err, ErrEmptyModule) {
		t.Fatalf("expected ErrEmptyModule, got: %v", err)
	}
}

// TestBuildStoresNotNoneRejected 验证 v0.1.0 stub：Stores!=None 返回错误。
func TestBuildStoresNotNoneRejected(t *testing.T) {
	_, err := Build(context.Background(), Spec{
		Module: "test-aggregate",
		Stores: All,
	})
	if err == nil {
		t.Fatal("expected error for Stores!=None in v0.1.0 stub, got nil")
	}
}

// TestBuildNilContext 验证 ctx 为 nil 返回错误。
func TestBuildNilContext(t *testing.T) {
	_, err := Build(nil, Spec{
		Module: "test",
		Stores: None,
	})
	if err == nil {
		t.Fatal("expected error for nil context, got nil")
	}
}

// TestShutdownIdempotent 验证 Shutdown 幂等（多次调用不 panic）。
func TestShutdownIdempotent(t *testing.T) {
	app, err := Build(context.Background(), Spec{
		Module: "test-idempotent",
		Stores: None,
	})
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Shutdown 在 Start 之前调用应安全返回 nil（未 Start）
	if err := app.Shutdown(context.Background()); err != nil {
		t.Errorf("first Shutdown before Start: %v", err)
	}
	// 第二次调用也应安全
	if err := app.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown: %v", err)
	}
}

// TestStoreSetHas 验证 StoreSet 位掩码 Has 方法。
func TestStoreSetHas(t *testing.T) {
	s := TD | PG | Redis
	if !s.Has(TD) {
		t.Error("expected TD set")
	}
	if !s.Has(PG) {
		t.Error("expected PG set")
	}
	if !s.Has(Redis) {
		t.Error("expected Redis set")
	}
	if s.Has(Kafka) {
		t.Error("expected Kafka not set")
	}
	if s.Has(NATS) {
		t.Error("expected NATS not set")
	}
}

// TestStoreSetString 验证 StoreSet.String 可读表示。
func TestStoreSetString(t *testing.T) {
	tests := []struct {
		stores StoreSet
		want   string
	}{
		{None, "None"},
		{All, "All"},
		{TD | PG, "TD|PG"},
		{Redis | Kafka, "Redis|Kafka"},
	}
	for _, tt := range tests {
		got := tt.stores.String()
		if got != tt.want {
			t.Errorf("StoreSet(%b).String() = %q, want %q", tt.stores, got, tt.want)
		}
	}
}

// TestCloserComponent 验证 closerComponent 实现 lifecycx.Component。
func TestCloserComponent(t *testing.T) {
	called := false
	c := newCloserComponent("test", func(ctx context.Context) error {
		called = true
		return nil
	})

	if c.Name() != "test" {
		t.Errorf("Name() = %q, want %q", c.Name(), "test")
	}
	if err := c.Start(context.Background()); err != nil {
		t.Errorf("Start() error: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop() error: %v", err)
	}
	if !called {
		t.Error("Stop did not call underlying close")
	}
}

// TestCloserComponentNilClose 验证 nil close 函数安全。
func TestCloserComponentNilClose(t *testing.T) {
	c := newCloserComponent("test", nil)
	if err := c.Stop(context.Background()); err != nil {
		t.Errorf("Stop with nil close: %v", err)
	}
}

// TestSpecValidate 验证 Spec.Validate。
func TestSpecValidate(t *testing.T) {
	if err := (Spec{}).Validate(); !errorsIs(err, ErrEmptyModule) {
		t.Errorf("empty Spec Validate: want ErrEmptyModule, got %v", err)
	}
	if err := (Spec{Module: "x"}).Validate(); err != nil {
		t.Errorf("valid Spec Validate: %v", err)
	}
}

// errorsIs 是 errors.Is 的本地包装（避免 test 文件直接 import errors 仅为断言）。
func errorsIs(err, target error) bool {
	if err == nil || target == nil {
		return err == target
	}
	// 简单相等检查（ErrEmptyModule 是 sentinel，直接 ==）
	for {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
		if err == nil {
			return false
		}
	}
}
