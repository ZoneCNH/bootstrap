#!/usr/bin/env bash
# bootstrap boundary-gates — 6 道 CI 边界门禁。
#
# 对应 module/bootstrap/SPEC.md §20。
# 用法：./scripts/boundary-gates.sh   # 任意一道失败 exit 1，全过 exit 0
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

pass=0
fail=0
gates_failed=()

run_gate() {
  local id="$1" name="$2"
  shift 2
  if "$@" >/tmp/gate-out 2>&1; then
    echo "PASS  $id  $name"
    pass=$((pass + 1))
  else
    echo "FAIL  $id  $name"
    sed 's/^/      /' /tmp/gate-out >&2 || true
    fail=$((fail + 1))
    gates_failed+=("$id")
  fi
}

# §20.1 禁业务语义：go.mod 无 domain-market/domain-macro/domainx/contracts
gate_no_business_semantics() {
  ! grep -qE 'ZoneCNH/(domain-market|domain-macro|domainx|contracts)' go.mod
}

# §20.2 禁采集逻辑：go.mod 无数据域子模块（binance/fred/…）
gate_no_data_domain() {
  ! grep -qE 'ZoneCNH/(binance|fred|bea|ecb|treasury|okx|bybit|bitget|gate|htx|coinbase|hyperliquid|kucoin|mexc|coinglass|eastmoney|jin10|yahoo|yield-curve|uk-cb|japan-cb|lighter|upbit)' go.mod
}

# §20.3 禁 transport 实体：源码无 net.Listen
gate_no_transport() {
  ! grep -Rn 'net\.Listen' --include='*.go' pkg/ 2>/dev/null
}

# §20.4 依赖方向：只向下依赖 kernel/configx/observex/resiliencx/存储适配器
# v0.2.0：foundationx 从白名单移除（源码已零 import，仅 observex indirect 残留）
gate_dependency_direction() {
  local hits
  hits="$(grep -Rn 'ZoneCNH/' --include='*.go' pkg/ 2>/dev/null | grep -vE 'kernel|configx|observex|resiliencx|taosx|postgresx|redisx|kafkax|natsx|ossx|clickhousex' || true)"
  [ -z "$hits" ]
}

# §20.5 foundationx 零命中：v0.2.0 清零后，bootstrap 源码不得 import foundationx
# （foundationx 仍可能作为 observex 等 primitive 的 indirect 依赖出现在 go.mod，
# 但 bootstrap 代码路径不引用它）。检查 import 语句，忽略注释。
gate_no_foundationx_import() {
  local hits
  hits="$(grep -Rn '"github.com/ZoneCNH/foundationx' --include='*.go' pkg/ 2>/dev/null || true)"
  [ -z "$hits" ]
}

# §20.6 编译通过
gate_build() {
  go build ./...
}

run_gate "20.1" "no-business-semantics"   gate_no_business_semantics
run_gate "20.2" "no-data-domain"          gate_no_data_domain
run_gate "20.3" "no-transport"            gate_no_transport
run_gate "20.4" "dependency-direction"    gate_dependency_direction
run_gate "20.5" "no-foundationx-import"   gate_no_foundationx_import
run_gate "20.6" "build"                   gate_build

echo ""
echo "Results: $pass passed, $fail failed"

if [ "$fail" -gt 0 ]; then
  echo "Failed gates: ${gates_failed[*]}"
  exit 1
fi
exit 0
