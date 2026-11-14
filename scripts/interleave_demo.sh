#!/usr/bin/env bash
# 可控“轮转 × 采集”交错演示（Docker Compose）。
#
# 流程：
#   1. 构建并启动 nginx + collector + traffic（持续产生 JSON 访问日志）；
#   2. 在采集器高频轮询（0.5s）的同时，快速连续执行若干次 rename+reopen
#      轮转，制造轮转落在“扫描后/打开前”和“打开后/读取前”的真实交错；
#   3. 等待采集追平后，用 /ingest/progress 与 /events 核对：
#        - 每个被追踪文件 offset == size（不漏行）；
#        - 每个物理位置仅入仓一次（不重记）；
#        - 每个归档作为独立来源保留、不截断；
#        - 按路径 / 状态码查询可用，/docs 可打开。
#
# 用法: ./scripts/interleave_demo.sh [轮转次数，默认 6]
set -euo pipefail
cd "$(dirname "$0")/.."

ROTATIONS="${1:-6}"

echo "==> 构建并启动环境（nginx / collector / traffic）"
docker compose build
docker compose up -d

cleanup() {
  echo
  echo "==> 演示结束，保留环境以便继续查看；停止请执行: docker compose down -v"
}
trap cleanup EXIT

echo "==> 等待 collector 就绪"
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://localhost:8000/health | python3 -m json.tool

echo
echo "==> 在采集进行中快速连续轮转 ${ROTATIONS} 次（制造交错）"
for i in $(seq 1 "$ROTATIONS"); do
  docker compose exec -T nginx \
    env ROTATE_NAME="interleave-${i}" \
    /usr/local/bin/rotate-nginx-log.sh >/dev/null
  echo "    rotation #${i} done"
  sleep 0.2   # 远小于/接近轮询周期，刻意制造重叠
done

echo
echo "==> 等待采集追平（offset 全部等于 size）"
for _ in $(seq 1 30); do
  pending=$(curl -fsS http://localhost:8000/ingest/progress \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for f in d["files"] if f["size"] is not None and f["offset"] < f["size"]))')
  [ "$pending" = "0" ] && break
  sleep 1
done

echo
echo "==> 逐文件进度（每个来源 offset 必须等于 size）"
curl -fsS http://localhost:8000/ingest/progress | python3 -c '
import json, sys
d = json.load(sys.stdin)
ok = True
for f in d["files"]:
    good = f["size"] is not None and f["offset"] == f["size"]
    ok = ok and good
    print(f"  [{\"complete\" if good else \"PENDING\":8}] "
          f"{f[\"source_code\"]:<36} inode={f[\"inode\"]:<8} "
          f"offset={f[\"offset\"]} size={f[\"size\"]}")
print("counts:", d["counts"])
sys.exit(0 if ok else 1)
'

echo
echo "==> 物理行唯一性核对（distinct 位置数应等于事件总数）"
docker compose exec -T collector python -c '
import sqlite3, os
c = sqlite3.connect(os.environ["DB_PATH"])
total = c.execute("SELECT COUNT(*) FROM log_events").fetchone()[0]
distinct = c.execute("SELECT COUNT(*) FROM (SELECT DISTINCT dev,inode,source_code,byte_offset FROM log_events)").fetchone()[0]
sources = c.execute("SELECT COUNT(DISTINCT source_code) FROM log_events").fetchone()[0]
print(f"  events={total}  distinct_positions={distinct}  distinct_sources={sources}")
assert total == distinct, "duplicate physical lines detected!"
print("  OK: 每个物理行仅入仓一次；归档来源各自独立保留")
'

echo
echo "==> 查询功能抽查"
echo "  5xx 故障请求:"
curl -fsS "http://localhost:8000/events?status_min=500&status_max=599&limit=5" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(f"    total={d[\"total\"]}"); [print(f"    {i[\"status\"]} {i[\"path\"]} src={i[\"source_code\"]}") for i in d["items"]]'
echo "  按路径 /missing/page 查询:"
curl -fsS "http://localhost:8000/events?path=/missing/page&limit=3" \
  | python3 -c 'import json,sys; print("    total =", json.load(sys.stdin)["total"])'

echo
echo "==> 完成。Swagger 文档: http://localhost:8000/docs"
