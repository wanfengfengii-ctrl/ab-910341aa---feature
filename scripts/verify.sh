#!/bin/sh
# 一键验证：镜像构建 -> 代码测试 -> API 冒烟（含分页中途追加早时刻数据，
# 以及分页中途执行质控勘误：旧游标见旧版、新会话见新版、过期修订号 409）。
# 验证始终在【清洁数据卷】上进行：启动前删除既有命名卷，避免历史数据干扰。
# 任一步失败，脚本即以该步退出码结束（可直接用于 CI）。
set -eu

cd "$(dirname "$0")/.."

cleanup() {
  echo "==> 清理（停止服务并删除数据卷，保持下次 verify 清洁）"
  docker compose down --remove-orphans --volumes || true
}
trap cleanup EXIT

echo "==> [1/2] 构建镜像（api + verify）"
docker compose build

echo "==> [2/2] 清洁数据卷启动并运行 verify（代码测试 + API 冒烟）"
# 显式清除可能存在的旧卷，确保本次 verify 从空库开始
docker compose down --remove-orphans --volumes >/dev/null 2>&1 || true
# 只启动 verify 及其依赖 api；--exit-code-from 透传一次性 verify 的退出码
docker compose up \
  --build \
  --abort-on-container-exit \
  --exit-code-from verify \
  verify
