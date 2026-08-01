#!/usr/bin/env python3
"""Đăng ký 3 dataset marts vào Superset qua REST API (idempotent).

    python scripts/register_superset_datasets.py

VÌ SAO CÓ SCRIPT NÀY:
  `reset_all.sh` xoá volume Postgres -> metadata Superset mất theo -> 3 dataset
  phải đăng ký lại. Trước đây làm TAY sau mỗi lần reset (điểm đau lặp đi lặp lại,
  và verify_2 fail vì nó). Script hoá để reset trở thành thao tác MỘT lệnh.

  Database connection "GTL Marts" đã do superset/bootstrap.sh tạo sẵn (trỏ marts_ro,
  read-only). Script này chỉ thêm phần dataset trỏ vào các bảng đã push.
"""

import pathlib
import sys

import http.cookiejar
import json
import urllib.error
import urllib.request

# Superset yêu cầu CSRF token + cookie phiên cho mọi POST -> dùng opener có cookie jar.
_OPENER = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
)

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
SUPERSET = "http://localhost:8088"
DATABASE_NAME = "GTL Marts"
SCHEMA = "public"          # push_marts.py ghi vào schema public của db `marts`
DATASETS = ["mart_daily_volume", "mart_channel_daily", "mart_category_daily"]


def load_env() -> dict:
    env = {}
    for line in (PROJECT_ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def call(method: str, path: str, token: str = None, payload: dict = None,
         csrf: str = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"{SUPERSET}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if csrf:
        req.add_header("X-CSRFToken", csrf)
        req.add_header("Referer", SUPERSET)
    try:
        with _OPENER.open(req, timeout=30) as resp:
            return json.loads(resp.read() or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()[:200]
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {body}") from exc


def main() -> int:
    env = load_env()
    token = call("POST", "/api/v1/security/login", payload={
        "username": "admin",
        "password": env["SUPERSET_ADMIN_PASSWORD"],
        "provider": "db",
        "refresh": True,
    })["access_token"]
    csrf = call("GET", "/api/v1/security/csrf_token/", token)["result"]

    databases = call("GET", "/api/v1/database/", token)["result"]
    db_id = next((d["id"] for d in databases if d["database_name"] == DATABASE_NAME), None)
    if db_id is None:
        print(f"❌ không thấy database '{DATABASE_NAME}' — superset/bootstrap.sh chưa chạy?")
        return 1

    existing = {d["table_name"] for d in call("GET", "/api/v1/dataset/", token)["result"]}
    created = skipped = 0
    for table in DATASETS:
        if table in existing:
            print(f"  = {table} (đã có, bỏ qua)")
            skipped += 1
            continue
        call("POST", "/api/v1/dataset/", token, {
            "database": db_id, "schema": SCHEMA, "table_name": table,
        }, csrf=csrf)
        print(f"  + {table} (đã tạo)")
        created += 1

    print(f"\n✅ dataset Superset: {created} tạo mới, {skipped} đã có "
          f"-> tổng {created + skipped}/{len(DATASETS)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
