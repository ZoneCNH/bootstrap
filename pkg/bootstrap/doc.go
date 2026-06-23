// Package bootstrap 是 L1 通用进程组装层。
//
// 它封装所有数据域进程（adapter + 聚合层）共有的
// configx 加载 + observex 初始化 + lifecycx 生命周期编排。
// 存储适配器作为聚合层的可选件按 StoreSet 位掩码启用。
//
// 典型用法（adapter，零存储）：
//
//	app, _ := bootstrap.Build(ctx, bootstrap.Spec{Module: "binance", Stores: bootstrap.None})
//	defer app.Shutdown(ctx)
//	app.Run(ctx)
//
// 典型用法（聚合层，全存储 + Hook 注入领域组件）：
//
//	app, _ := bootstrap.Build(ctx, bootstrap.Spec{
//	    Module: "market-data",
//	    Stores: bootstrap.All,
//	    Hooks: []func(*bootstrap.App) error{
//	        func(app *bootstrap.App) error {
//	            app.Register(myDomainComponent{...}) // 挂载领域 server/admin
//	            return nil
//	        },
//	    },
//	})
//	defer app.Shutdown(ctx)
//	app.Run(ctx)
//
// 对齐 module/bootstrap/SPEC.md v0.2.0。
package bootstrap
