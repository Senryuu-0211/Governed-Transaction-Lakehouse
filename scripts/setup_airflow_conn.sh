#!/usr/bin/env bash
# =============================================================================
# Dựng cầu SSH worker(Airflow) -> host: key riêng + Airflow connection.
# Idempotent — chạy lại nhiều lần an toàn. CHẠY TRÊN HOST (user senryuu).
# =============================================================================
set -euo pipefail
KEY="$HOME/.ssh/gtl_airflow_ed25519"
WORKER="airflow-docker-airflow-worker-1"
CONN="gtl_host_ssh"

# 1. Key riêng cho Airflow (tách key cá nhân -> thu hồi độc lập)
[ -f "$KEY" ] || ssh-keygen -t ed25519 -N '' -C 'gtl-airflow' -f "$KEY"

# 2. Cho phép key này SSH vào chính host (idempotent)
PUB="$(cat "$KEY.pub")"
touch "$HOME/.ssh/authorized_keys"; chmod 600 "$HOME/.ssh/authorized_keys"
grep -qF "$PUB" "$HOME/.ssh/authorized_keys" || echo "$PUB" >> "$HOME/.ssh/authorized_keys"

# 3. Gateway host nhìn từ worker (động — không hard-code subnet docker)
GW="$(docker inspect "$WORKER" --format '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}')"
echo "host gateway (từ worker) = $GW"

# 4. Airflow connection: private key + tắt strict host-key (LAN Tailscale tin cậy)
EXTRA="$(python3 - "$KEY" <<'PY'
import json, sys
key = open(sys.argv[1]).read()
print(json.dumps({"private_key": key, "no_host_key_check": True, "conn_timeout": 30}))
PY
)"
docker exec "$WORKER" airflow connections delete "$CONN" >/dev/null 2>&1 || true
docker exec "$WORKER" airflow connections add "$CONN" \
  --conn-type ssh --conn-host "$GW" --conn-login senryuu --conn-extra "$EXTRA"

# 5. Kiểm SSH thông: worker -> host chạy whoami
echo "-- test SSH worker -> host --"
docker exec "$WORKER" python -c "
from airflow.providers.ssh.hooks.ssh import SSHHook
c = SSHHook(ssh_conn_id='$CONN').get_conn()
_, out, err = c.exec_command('whoami')
who = out.read().decode().strip()
print('remote whoami =', who or err.read().decode())
assert who == 'senryuu', 'SSH bridge FAIL'
print('OK: SSH bridge worker -> host hoạt động')
"
