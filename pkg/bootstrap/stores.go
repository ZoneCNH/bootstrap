package bootstrap

// 存储 adapter 构造。
//
// SPEC OQ-003 结论：6 个 adapter（taosx/postgresx/redisx/kafkax/natsx/clickhousex）
// 有真实源码且 New 签名统一为 New(ctx, Config, opts...)。ossx 当前 0 源码（纯文档），
// 构造时跳过并记录 warning。
//
// 每个 adapter 的 Config 字段各异，stores.go 负责从进程环境变量
// （XGO_{MODULE}_{STORE}_*）解码为对应 adapter 的 Config。
//
// 构造后的 Client 用 closerComponent wrapper 注册进 lifecycx.Manager。

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ZoneCNH/clickhousex/pkg/clickhousex"
	"github.com/ZoneCNH/foundationx/pkg/foundationx"
	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/ZoneCNH/natsx/pkg/natsx"
	"github.com/ZoneCNH/postgresx/pkg/postgresx"
	"github.com/ZoneCNH/redisx/pkg/redisx"
	"github.com/ZoneCNH/taosx/pkg/taosx"
)

// buildStores 按 Spec.Stores 位掩码构造启用的存储适配器。
// 未启用的位为 nil。返回 nil + nil error 表示零存储（adapter）。
//
// 各 adapter Config 从环境变量 XGO_{MODULE}_{STORE}_* 解码。
func buildStores(ctx context.Context, spec Spec) (*Stores, error) {
	if spec.Stores == None {
		return nil, nil
	}

	stores := &Stores{}
	prefix := "XGO_" + upper(spec.Module) + "_"
	var errs []error

	if spec.Stores.Has(TD) {
		c, err := buildTaos(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("taosx: %w", err))
		} else {
			stores.TD = c
		}
	}
	if spec.Stores.Has(PG) {
		c, err := buildPostgres(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("postgresx: %w", err))
		} else {
			stores.PG = c
		}
	}
	if spec.Stores.Has(Redis) {
		c, err := buildRedis(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("redisx: %w", err))
		} else {
			stores.Redis = c
		}
	}
	if spec.Stores.Has(Kafka) {
		c, err := buildKafka(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("kafkax: %w", err))
		} else {
			stores.Kafka = c
		}
	}
	if spec.Stores.Has(NATS) {
		c, err := buildNATS(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("natsx: %w", err))
		} else {
			stores.NATS = c
		}
	}
	if spec.Stores.Has(OSS) {
		// ossx 当前 0 源码（纯文档），跳过构造。
		// stores.OSS 保持 nil；待 ossx 发布运行时后补全。
		stores.OSS = nil
	}
	if spec.Stores.Has(CH) {
		c, err := buildClickHouse(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("clickhousex: %w", err))
		} else {
			stores.CH = c
		}
	}

	if len(errs) > 0 {
		return stores, fmt.Errorf("stores construction failed: %v", errs)
	}
	return stores, nil
}

// storeComponents 把已构造的 Stores 转为 closerComponent 列表（注册进 Lifecycle）。
func (s *Stores) components(module string) []closerComponent {
	var comps []closerComponent
	if s == nil {
		return comps
	}
	if s.TD != nil {
		if c, ok := s.TD.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":td", c.Close))
		}
	}
	if s.PG != nil {
		if c, ok := s.PG.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":pg", c.Close))
		}
	}
	if s.Redis != nil {
		if c, ok := s.Redis.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":redis", c.Close))
		}
	}
	if s.Kafka != nil {
		if c, ok := s.Kafka.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":kafka", c.Close))
		}
	}
	if s.NATS != nil {
		if c, ok := s.NATS.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":nats", c.Close))
		}
	}
	if s.CH != nil {
		if c, ok := s.CH.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":ch", c.Close))
		}
	}
	return comps
}

// ---- 环境变量解码辅助 ----

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}
	return fallback
}

func envSlice(key string, fallback []string) []string {
	if v := os.Getenv(key); v != "" {
		return strings.Split(v, ",")
	}
	return fallback
}

// ---- 6 个 adapter 构造函数 ----
//
// 每个 build* 函数从 XGO_{MODULE}_{STORE}_* 环境变量解码对应 adapter 的 Config，
// 调用 adapter 的 New(ctx, Config) 构造 Client。
//
// import 各 adapter 的 pkg/{name} 子包。

// buildTaos 构造 taosx Client（interface 类型，非指针）。
func buildTaos(ctx context.Context, prefix string) (any, error) {
	cfg := taosx.Config{
		Name:     envOr(prefix+"TD_NAME", "bootstrap"),
		Endpoint: envOr(prefix+"TD_ENDPOINT", "127.0.0.1:6041"),
		Database: envOr(prefix+"TD_DATABASE", ""),
		Username: envOr(prefix+"TD_USER", "root"),
		Password: envOr(prefix+"TD_PASSWORD", ""),
		Timeout:  envDuration(prefix+"TD_TIMEOUT", 10*time.Second),
		TLS:      envBool(prefix+"TD_TLS", false),
	}
	return taosx.New(ctx, cfg)
}

// buildPostgres 构造 postgresx *Client。
func buildPostgres(ctx context.Context, prefix string) (any, error) {
	cfg := postgresx.Config{
		Host:           envOr(prefix+"PG_HOST", "127.0.0.1"),
		Port:           envInt(prefix+"PG_PORT", 5432),
		Database:       envOr(prefix+"PG_DATABASE", ""),
		User:           envOr(prefix+"PG_USER", ""),
		Password:       foundationx.SecretString(envOr(prefix+"PG_PASSWORD", "")),
		SSLMode:        envOr(prefix+"PG_SSLMODE", "disable"),
		MaxOpenConns:   int32(envInt(prefix+"PG_MAX_OPEN_CONNS", 25)),
		MinIdleConns:   int32(envInt(prefix+"PG_MIN_IDLE_CONNS", 5)),
		ConnectTimeout: envDuration(prefix+"PG_CONNECT_TIMEOUT", 10*time.Second),
	}
	return postgresx.New(ctx, cfg)
}

// buildRedis 构造 redisx *Client。
func buildRedis(ctx context.Context, prefix string) (any, error) {
	cfg := redisx.Config{
		Name:    envOr(prefix+"REDIS_NAME", "bootstrap"),
		Timeout: envDuration(prefix+"REDIS_TIMEOUT", 5*time.Second),
		Redis: redisx.RedisConfig{
			Addr:     envOr(prefix+"REDIS_ADDR", "127.0.0.1:6379"),
			Password: envOr(prefix+"REDIS_PASSWORD", ""),
			DB:       envInt(prefix+"REDIS_DB", 0),
		},
	}
	return redisx.New(ctx, cfg)
}

// buildKafka 构造 kafkax *Client。
func buildKafka(ctx context.Context, prefix string) (any, error) {
	cfg := kafkax.Config{
		Name:     envOr(prefix+"KAFKA_NAME", "bootstrap"),
		Timeout:  envDuration(prefix+"KAFKA_TIMEOUT", 10*time.Second),
		Brokers:  envSlice(prefix+"KAFKA_BROKERS", []string{"127.0.0.1:9092"}),
		ClientID: envOr(prefix+"KAFKA_CLIENT_ID", "bootstrap"),
		Security: kafkax.SecurityConfig{
			Protocol: kafkax.SecurityProtocol(envOr(prefix+"KAFKA_SASL", "plaintext")),
			Username: envOr(prefix+"KAFKA_USER", ""),
			Password: envOr(prefix+"KAFKA_PASSWORD", ""),
		},
	}
	return kafkax.New(ctx, cfg)
}

// buildNATS 构造 natsx *Client。
func buildNATS(ctx context.Context, prefix string) (any, error) {
	cfg := natsx.Config{
		Name:          envOr(prefix+"NATS_NAME", "bootstrap"),
		URL:           envOr(prefix+"NATS_URL", "nats://127.0.0.1:4222"),
		Username:      envOr(prefix+"NATS_USER", ""),
		Password:      envOr(prefix+"NATS_PASSWORD", ""),
		Timeout:       envDuration(prefix+"NATS_TIMEOUT", 5*time.Second),
		DrainTimeout:  envDuration(prefix+"NATS_DRAIN_TIMEOUT", 10*time.Second),
		MaxReconnects: envInt(prefix+"NATS_MAX_RECONNECTS", 60),
	}
	return natsx.New(ctx, cfg)
}

// buildClickHouse 构造 clickhousex *Client。
func buildClickHouse(ctx context.Context, prefix string) (any, error) {
	cfg := clickhousex.Config{
		Name:            envOr(prefix+"CH_NAME", "bootstrap"),
		Host:            envOr(prefix+"CH_HOST", "127.0.0.1"),
		Port:            envInt(prefix+"CH_PORT", 9000),
		Database:        envOr(prefix+"CH_DATABASE", "default"),
		Username:        envOr(prefix+"CH_USER", "default"),
		Password:        envOr(prefix+"CH_PASSWORD", ""),
		MaxOpenConns:    envInt(prefix+"CH_MAX_OPEN_CONNS", 10),
		MaxIdleConns:    envInt(prefix+"CH_MAX_IDLE_CONNS", 5),
		ConnMaxLifetime: envDuration(prefix+"CH_CONN_MAX_LIFETIME", time.Hour),
	}
	return clickhousex.New(ctx, cfg)
}
