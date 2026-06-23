# bootstrap

ZoneCNH bootstrap — L1 通用进程组装层。

## 角色

`github.com/ZoneCNH/bootstrap` 封装所有数据域进程（adapter + 聚合层）共有的 **configx 加载 + observex 初始化 + lifecycx 生命周期编排**。存储适配器作为聚合层的可选件按 `StoreSet` 位掩码启用。

```
进程 main
  app, _ := bootstrap.Build(ctx, bootstrap.Spec{Module: "binance", Stores: bootstrap.None})
  defer app.Shutdown(ctx)
  app.Run(ctx)
```

## 当前状态：v0.2.0（7 存储全接入 + foundationx 清零 + Hook 注入点）

- ✅ `Build` / `Run` / `Shutdown` 核心入口
- ✅ `Spec` / `StoreSet` / `App` / `Stores` 类型定义（`Stores` 字段强类型，SPEC §9.1 契约）
- ✅ `closerComponent` wrapper（把基座 Client Close 适配为 lifecycx.Component）
- ✅ configx 加载（.env + EnvSource）+ observex/resiliencx 初始化
- ✅ `Build` 返回 `app.Stores`，聚合层可在 Hook 和 Build 返回后访问存储句柄
- ✅ **7 个存储适配器全部构造**：taosx / postgresx / redisx / kafkax / natsx / ossx(aliyun) / clickhousex
- ✅ `Spec.Hooks` + `app.Register` 注入点：子模块在 Build 内挂载领域 server/admin（domainx/contracts 经此注入，bootstrap 本身不 import 领域层）
- ✅ foundationx 直接依赖清零（OQ-004 关闭；源码零 import，postgresx@v1.1.0 自带 SecretString）
- ✅ 信号捕获（kernel.shutdownx）+ 逆序 Stop
- ✅ 6 道边界门禁（boundary-gates.sh，含 foundationx 零命中门禁）
- ✅ go build + go vet + go test -race 全过

### 明确不做（留后续迭代）

- 从 observex Client 暴露 logger/metrics（基座无 getter，OQ-001 已确认）
- admin HTTP / metrics endpoint（各服务自己的事，经 Hook 注册）
- graceful shutdown 超时策略（用 kernel.shutdownx）
- import 领域共享层 domainx/contracts（分层铁律：L1 不向上依赖；由子模块经 Hook 注入）

## 目录结构

```
pkg/bootstrap/
  ├── doc.go              包文档
  ├── spec.go             Spec / StoreSet / App / Stores / 错误
  ├── component.go        closerComponent wrapper
  ├── bootstrap.go        Build / Run / Shutdown + 内部构造
  ├── version.go          版本常量
  └── bootstrap_test.go   单元测试
scripts/
  └── boundary-gates.sh   6 道 CI 边界门禁
```

## 构建 / 测试

```bash
go build ./...
go test ./... -race -count=1
./scripts/boundary-gates.sh
```

## 设计文档

完整规格位于上游规格仓库：

- `module/bootstrap/SPEC.md` — 23 节完整规格（v0.2.0）

## 边界纪律

- bootstrap 禁 import domain-market/domain-macro/domainx/contracts（禁业务语义；领域层由子模块经 Hook 注入）
- bootstrap 禁 import 数据域子模块（禁采集逻辑）
- bootstrap 不起 HTTP/gRPC server（源码无 net.Listen）
- bootstrap 只向下依赖 kernel/configx/observex/resiliencx/存储适配器
- bootstrap 源码禁 import foundationx（boundary-gates §20.5 硬门禁；indirect 残留不算违规）
