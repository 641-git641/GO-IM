#!/usr/bin/env bash
# =============================================================================
# IM 项目 — Let's Encrypt SSL 证书初始化脚本
#
# 用途: 首次获取 SSL 证书（之后由 Docker certbot 自动续期）
# 用法:
#   1. 确保你的域名 DNS 已解析到本服务器 IP
#   2. 确保端口 80 已开放（云服务商安全组 + 服务器防火墙）
#   3. 替换 YOUR_DOMAIN 为实际域名
#   4. bash deploy/init-ssl.sh your-domain.com
#
# 架构说明:
#   首次获取证书时，不能使用 HTTPS（证书还没拿到），所以脚本会临时启动
#   一个仅监听 80 端口的 nginx，用于完成 Let's Encrypt HTTP-01 验证。
#   获取成功后，再启动完整的 docker-compose.prod.yml（含 HTTPS）。
# =============================================================================

set -euo pipefail

# ---- 配置 ----
DOMAIN="${1:-}"
if [ -z "$DOMAIN" ]; then
    echo "用法: bash deploy/init-ssl.sh <your-domain.com>"
    echo "示例: bash deploy/init-ssl.sh im.example.com"
    exit 1
fi

EMAIL="${2:-admin@$DOMAIN}"

echo "============================================"
echo " IM 项目 — SSL 证书初始化"
echo " 域名:   $DOMAIN"
echo " 邮箱:   $EMAIL"
echo "============================================"
echo ""

# ---- 准备工作 ----
echo "[1/5] 创建证书存放目录..."
CERTBOT_DIR="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$CERTBOT_DIR/certbot/www"
mkdir -p "$CERTBOT_DIR/certbot/conf"

# ---- 获取证书（dry-run 先测试，确保 ACME 可达）----
echo "[2/5] 测试 ACME 验证（dry-run）..."
docker run --rm \
    -v "$CERTBOT_DIR/certbot/www:/var/www/certbot:rw" \
    -v "$CERTBOT_DIR/certbot/conf:/etc/letsencrypt:rw" \
    -p 80:80 \
    certbot/certbot \
    certonly \
    --webroot -w /var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    -d "$DOMAIN" \
    --dry-run

echo ""
echo "[3/5] 测试通过，正式获取证书..."
docker run --rm \
    -v "$CERTBOT_DIR/certbot/www:/var/www/certbot:rw" \
    -v "$CERTBOT_DIR/certbot/conf:/etc/letsencrypt:rw" \
    -p 80:80 \
    certbot/certbot \
    certonly \
    --webroot -w /var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    -d "$DOMAIN" \
    --force-renewal

echo ""
echo "[4/5] 检查证书文件..."
if [ -f "$CERTBOT_DIR/certbot/conf/live/$DOMAIN/fullchain.pem" ]; then
    echo "  ✅ 证书获取成功！"
    echo "  证书路径: $CERTBOT_DIR/certbot/conf/live/$DOMAIN/"
else
    echo "  ❌ 证书获取失败，请检查："
    echo "    1. 域名 DNS 是否已解析到本服务器 IP"
    echo "    2. 端口 80 是否已开放（云服务商安全组）"
    echo "    3. 服务器防火墙是否放行 80 端口"
    exit 1
fi

# ---- 替换 nginx 配置中的域名占位符 ----
echo "[5/5] 生成 nginx 配置..."
sed "s/\${DOMAIN}/$DOMAIN/g" "$CERTBOT_DIR/nginx.prod.conf" > "$CERTBOT_DIR/nginx.prod.generated.conf"
echo "  已生成: deploy/nginx.prod.generated.conf"

echo ""
echo "============================================"
echo " ✅ SSL 证书初始化完成！"
echo ""
echo " 接下来请执行："
echo "  1. 编辑 docker-compose.prod.yml 中 proxy 的 volumes，"
echo "     将 nginx.prod.conf 改为 nginx.prod.generated.conf"
echo "  2. 修改 configs/config.docker.json 中的 jwt.secret"
echo "  3. 启动: docker-compose -f docker-compose.prod.yml up -d"
echo "============================================"
