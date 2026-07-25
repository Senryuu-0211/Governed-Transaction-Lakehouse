"""Superset config — nạp qua SUPERSET_CONFIG_PATH. KHÔNG chứa secret cứng
(secret + password đọc từ env do compose truyền từ .env)."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata DB của Superset = db `superset` trong gtl-postgres (không đụng banking/marts).
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset_meta:"
    f"{os.environ['SUPERSET_META_PASSWORD']}@postgres:5432/superset"
)

# Demo 1 node: tắt cache phân tán, chạy metadata cho đơn giản.
SUPERSET_LOAD_EXAMPLES = False
FEATURE_FLAGS = {"DASHBOARD_RBAC": True}
# Superset sau proxy/tailscale — cho phép nhúng iframe nội bộ, tắt CSRF host-check gắt.
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
