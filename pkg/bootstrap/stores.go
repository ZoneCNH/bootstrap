package bootstrap

// 存储 adapter 构造。
//
// SPEC OQ-003 结论：7 个 adapter（taosx/postgresx/redisx/kafkax/natsx/ossx/clickhousex）
// 均有真实源码。6 个（除 ossx）New 签名统一为 New(ctx, Config, opts...)；
// ossx 是唯一特例：NewBlobStore(cfg, adapter, hooks)，必须显式传入 StoreAdapter
// （SPEC §1 声明单一 provider，bootstrap 硬编码 adapters/aliyun）。
//
// 每个 adapter 的 Config 字段各异，stores.go 负责从进程环境变量
// （XGO_{MODULE}_{STORE}_*）解码为对应 adapter 的 Config。
//
// foundationx 直接依赖已于 v0.2.0 清零：postgresx@v1.1.0 自带 postgresx.SecretString
// （pkg/postgresx/secret.go），bootstrap 源码不再 import foundationx。
// 注：foundationx 仍可能作为 observex 等 L1 primitive 的 indirect 依赖出现在
// go.mod，但 bootstrap 代码路径不引用它（grep import 语句零命中）。
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
	"github.com/ZoneCNH/kafkax/pkg/kafkax"
	"github.com/ZoneCNH/natsx/pkg/natsx"
	"github.com/ZoneCNH/ossx/adapters/aliyun"
	"github.com/ZoneCNH/ossx/pkg/ossx"
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
		c, err := buildOSS(ctx, prefix)
		if err != nil {
			errs = append(errs, fmt.Errorf("ossx: %w", err))
		} else {
			stores.OSS = c
		}
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
//
// v0.2.0 强类型化后字段类型已知：taosx.Client / ossx.BlobStore 是 interface
// （需 type assert 取 Close），其余 5 个是具体 *Client（直接取方法值）。
// clickhousex 特例：Close() 无 ctx 参数，改用 CloseContext(ctx)。
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
		comps = append(comps, *newCloserComponent(module+":pg", s.PG.Close))
	}
	if s.Redis != nil {
		comps = append(comps, *newCloserComponent(module+":redis", s.Redis.Close))
	}
	if s.Kafka != nil {
		comps = append(comps, *newCloserComponent(module+":kafka", s.Kafka.Close))
	}
	if s.NATS != nil {
		comps = append(comps, *newCloserComponent(module+":nats", s.NATS.Close))
	}
	if s.OSS != nil {
		if c, ok := s.OSS.(interface{ Close(context.Context) error }); ok {
			comps = append(comps, *newCloserComponent(module+":oss", c.Close))
		}
	}
	if s.CH != nil {
		comps = append(comps, *newCloserComponent(module+":ch", s.CH.CloseContext))
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
func buildTaos(ctx context.Context, prefix string) (taosx.Client, error) {
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
//
// postgresx@v1.0.0 自带 SecretString（pkg/postgresx/secret.go），
// 不再依赖 foundationx（v0.2.0 清零，OQ-004 关闭）。
func buildPostgres(ctx context.Context, prefix string) (*postgresx.Client, error) {
	cfg := postgresx.Config{
		Host:           envOr(prefix+"PG_HOST", "127.0.0.1"),
		Port:           envInt(prefix+"PG_PORT", 5432),
		Database:       envOr(prefix+"PG_DATABASE", ""),
		User:           envOr(prefix+"PG_USER", ""),
		Password:       postgresx.NewSecretString(envOr(prefix+"PG_PASSWORD", "")),
		SSLMode:        envOr(prefix+"PG_SSLMODE", "disable"),
		MaxOpenConns:   int32(envInt(prefix+"PG_MAX_OPEN_CONNS", 25)),
		MinIdleConns:   int32(envInt(prefix+"PG_MIN_IDLE_CONNS", 5)),
		ConnectTimeout: envDuration(prefix+"PG_CONNECT_TIMEOUT", 10*time.Second),
	}
	return postgresx.New(ctx, cfg)
}

// buildRedis 构造 redisx *Client。
func buildRedis(ctx context.Context, prefix string) (*redisx.Client, error) {
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
func buildKafka(ctx context.Context, prefix string) (*kafkax.Client, error) {
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
func buildNATS(ctx context.Context, prefix string) (*natsx.Client, error) {
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
func buildClickHouse(ctx context.Context, prefix string) (*clickhousex.Client, error) {
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

// buildOSS 构造 ossx BlobStore。
//
// ossx 是 7 存储中唯一的特例：NewBlobStore(cfg, adapter, hooks) 需要显式传入
// StoreAdapter（其它 6 个 adapter 是 New(ctx, Config)）。
// SPEC §1 声明 ossx 单一 provider（Aliyun OSS），因此 bootstrap 硬编码
// adapters/aliyun.NewAdapter。若未来引入多 provider，应改为 Spec 可选项。
//
// env 前缀注意：ossx 自带 ConfigFromEnv 读 FOUNDATIONX_OSSX_*，
// bootstrap 不复用它，按统一约定解码 XGO_{MODULE}_OSS_*（与其它 6 个一致）。
func buildOSS(ctx context.Context, prefix string) (ossx.BlobStore, error) {
	cfg := ossx.Config{
		Endpoint:  envOr(prefix+"OSS_ENDPOINT", ""),
		Region:    envOr(prefix+"OSS_REGION", ""),
		Bucket:    envOr(prefix+"OSS_BUCKET", ""),
		PathStyle: envBool(prefix+"OSS_PATH_STYLE", false),
		UseSSL:    envBool(prefix+"OSS_USE_SSL", true),
		CNAME:     envOr(prefix+"OSS_CNAME", ""),
		AccessKey: envOr(prefix+"OSS_ACCESS_KEY", ""),
		SecretKey: envOr(prefix+"OSS_SECRET_KEY", ""),
		Timeouts: ossx.Timeouts{
			Connect:   envDuration(prefix+"OSS_CONNECT_TIMEOUT", 5*time.Second),
			Operation: envDuration(prefix+"OSS_OPERATION_TIMEOUT", 30*time.Second),
		},
		Multipart: ossx.MultipartPolicy{
			MinPartSize:    int64(envInt(prefix+"OSS_MULTIPART_MIN_PART", 8<<20)),
			MaxParts:       envInt(prefix+"OSS_MULTIPART_MAX_PARTS", 10000),
			MaxConcurrency: envInt(prefix+"OSS_MULTIPART_CONCURRENCY", 4),
		},
		Presign: ossx.PresignPolicy{
			MaxTTL:            envDuration(prefix+"OSS_PRESIGN_MAX_TTL", ossx.MaxAllowedPresignTTL),
			AllowedOperations: []ossx.PresignOperation{ossx.PresignGet, ossx.PresignPut},
		},
	}
	adapter, err := aliyun.NewAdapter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("aliyun adapter: %w", err)
	}
	return ossx.NewBlobStore(cfg, adapter, ossx.Hooks{})
}
