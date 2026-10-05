#!/bin/sh
# 一键验证：镜像构建 -> 清洁数据卷启动 -> 代码测试 -> API 冒烟
#   （含分页中途追加早时刻数据、分页中途执行质控勘误、并发冲突）。
# 任一步失败，脚本即以该步退出码结束（可直接用于 CI）。
set -eu

cd "$(dirname "$0")/.."

cleanup() {
  echo "==> 清理（含数据卷，保证每次 verify 都在清洁卷上运行）"
  docker compose down --remove-orphans -v || true
}
trap cleanup EXIT

echo "==> [1/2] 构建镜像（api + verify）"
docker compose build

echo "==> [2/2] 清洁启动并运行 verify（代码测试 + API 冒烟）"
# 先清掉可能残留的容器与命名数据卷：verify 必须从空库开始。
docker compose down --remove-orphans -v || true
# 只启动 verify 及其依赖 api；--exit-code-from 透传一次性 verify 的退出码
docker compose up \
  --build \
  --abort-on-container-exit \
  --exit-code-from verify \
  verify
