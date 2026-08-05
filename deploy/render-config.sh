#!/usr/bin/env bash
# =============================================================================
# IM 项目 — 生产配置渲染脚本(CD 专用)
#
# 用途: 将 deploy/config.prod.json 模板中的占位符替换为真实值,
#       输出 configs/config.prod.generated.json(gitignore,不会误提交)。
#
# 依赖环境变量(由 cd.yml 的 render 步骤注入 secrets):
#   DOMAIN           站点域名 → check_origin(证书域名一致)
#   JWT_SECRET       登录令牌签名密钥(openssl rand -base64 64 生成)
#   MYSQL_PASSWORD   应用连 MySQL 的密码 → DSN(须与服务器 .env 的 MYSQL_PASSWORD 一致)
#   MINIO_SECRET_KEY MinIO 访问密钥 → object_storage.secret_key
#                   (须与服务器 .env 的 MINIO_ROOT_PASSWORD 一致 —— 应用即用 root 访问)
#   ADMIN_UID        管理员 UID → admin_uids
#
# 注意:
#   1. 渲染目标为 gitignore 的 configs/config.prod.generated.json,
#      手动部署与 CD 流水线共用该产物(prod compose 的 CONFIG_PATH 即指向它)。
#   2. dev 环境不受影响,仍用 configs/config.docker.json。
# =============================================================================

set -euo pipefail

SRC="deploy/config.prod.json"
DST="configs/config.prod.generated.json"

# 必填环境变量校验
REQUIRED=(DOMAIN JWT_SECRET MYSQL_PASSWORD MINIO_SECRET_KEY ADMIN_UID)
for v in "${REQUIRED[@]}"; do
    if [ -z "${!v:-}" ]; then
        echo "ERROR: 缺少环境变量 $v(cd.yml 未注入对应 secret?)" >&2
        exit 1
    fi
done

# 转义 sed 替换串中的特殊字符(& \ 与分隔符 |)
esc() {
    printf '%s' "$1" | sed 's/[&\\|]/\\&/g'
}

mkdir -p "$(dirname "$DST")"

sed \
    -e "s|YOUR_DOMAIN|$(esc "$DOMAIN")|g" \
    -e "s|YOUR_ADMIN_UID|$(esc "$ADMIN_UID")|g" \
    -e "s|CHANGE_ME_JWT_SECRET_USE_RANDOM_64_CHARS|$(esc "$JWT_SECRET")|g" \
    -e "s|CHANGE_ME_PASSWORD|$(esc "$MYSQL_PASSWORD")|g" \
    -e "s|CHANGE_ME_MINIO_SECRET|$(esc "$MINIO_SECRET_KEY")|g" \
    "$SRC" > "$DST"

echo "✅ 已生成 $DST"
echo "   check_origin: https://${DOMAIN} / https://www.${DOMAIN}"
echo "   admin_uids:   [${ADMIN_UID}]"
echo "   剩余占位符检查(应为空):"
grep -nE 'YOUR_DOMAIN|YOUR_ADMIN_UID|CHANGE_ME' "$DST" || echo "   无 ✅"
