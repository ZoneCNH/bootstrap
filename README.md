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

## 当前状态：v0.1.0（最小可编译骨架）

- ✅ `Build` / `Run` / `Shutdown` 核心入口
- ✅ `Spec` / `StoreSet` / `App` 类型定义
- ✅ `closerComponent` wrapper（把基座 Client Close 适配为 lifecycx.Component）
- ✅ configx 加载（.env + EnvSource）+ observex/resiliencx 初始化
- ✅ 信号捕获（kernel.shutdownx）+ 逆序 Stop
- ✅ 5 道边界门禁（boundary-gates.sh）
- ✅ go build + go vet + go test 全过

### 首版明确不做（留后续迭代）

- 存储适配器构造（Stores!=None 返回明确错误；待 7 存储 adapter New 签名固化后补全）
- 从 observex Client 暴露 logger/metrics（基座无 getter，OQ-001 已确认）
- admin HTTP / metrics endpoint
- graceful shutdown 超时策略

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
  └── boundary-gates.sh   5 道 CI 边界门禁
```

## 构建 / 测试

```bash
go build ./...
go test ./... -race -count=1
./scripts/boundary-gates.sh
```

## 设计文档

完整规格位于上游规格仓库：

- `module/bootstrap/SPEC.md` — 23 节完整规格（v0.1.1）

## 边界纪律

- bootstrap 禁 import domain-market/domain-macro/domainx/contracts（禁业务语义）
- bootstrap 禁 import 数据域子模块（禁采集逻辑）
- bootstrap 不起 HTTP/gRPC server（源码无 net.Listen）
- bootstrap 只向下依赖 kernel/configx/observex/resiliencx/存储适配器
