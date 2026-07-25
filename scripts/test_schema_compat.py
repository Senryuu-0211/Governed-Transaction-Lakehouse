"""Schema Registry ENFORCE compatibility — điểm cốt lõi của Phase 2.5.

Làm 2 việc:
  1. HARDENING: đặt compatibility=BACKWARD lên 3 subject value THẬT
     (gtl.public.*-value) -> từ nay nguồn đổi schema không tương thích bị CHẶN
     ở cổng Kafka, thay vì để Silver lặng lẽ ra NULL.
  2. DEMO: trên một subject THROWAWAY (tự chứa, không đụng production), chứng minh
     - schema KHÔNG tương thích (đổi kiểu field) -> registry từ chối (409)
     - schema tương thích (thêm field optional có default) -> chấp nhận (200)
     rồi xoá subject demo.

Chạy:  ~/working/gtl-spark-venv/bin/python scripts/test_schema_compat.py
Exit != 0 nếu enforcement không đúng kỳ vọng.
"""
import json
import sys
import urllib.error
import urllib.request

CCOMPAT = "http://localhost:8087/apis/ccompat/v7"
REAL_SUBJECTS = [
    "gtl.public.transactions-value",
    "gtl.public.accounts-value",
    "gtl.public.merchants-value",
]
PROBE = "gtl.__compat_probe"
BASE = {
    "type": "record", "name": "Probe", "namespace": "gtl.test",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": "string"},
    ],
}


def _req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{CCOMPAT}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, (r.read().decode() or "")
    except urllib.error.HTTPError as e:
        return e.code, (e.read().decode() or "")


def register(subject, schema):
    return _req("POST", f"/subjects/{subject}/versions",
                {"schema": json.dumps(schema), "schemaType": "AVRO"})[0]


def main() -> int:
    ok = True

    # 1) HARDENING — bảo vệ 3 subject production thật.
    print("[1] Đặt BACKWARD lên subject production:")
    for s in REAL_SUBJECTS:
        _req("PUT", f"/config/{s}", {"compatibility": "BACKWARD"})
        _, cfg = _req("GET", f"/config/{s}")
        level = json.loads(cfg or "{}").get("compatibilityLevel")
        mark = "✅" if level == "BACKWARD" else "❌"
        print(f"      {s} = {level} {mark}")
        ok = ok and level == "BACKWARD"

    # 2) DEMO trên subject throwaway.
    print(f"\n[2] Demo enforcement trên subject throwaway ({PROBE}):")
    _req("DELETE", f"/subjects/{PROBE}")  # sạch nếu chạy lại
    code = register(PROBE, BASE)
    _req("PUT", f"/config/{PROBE}", {"compatibility": "BACKWARD"})
    print(f"      đăng ký base + set BACKWARD (HTTP {code})")

    bad = json.loads(json.dumps(BASE))
    bad["fields"][1]["type"] = "long"  # amount string->long: data cũ đọc gãy
    code = register(PROBE, bad)
    print(f"      schema KHÔNG tương thích (amount string->long) -> "
          f"{code} {'BỊ CHẶN ✅' if code == 409 else 'FAIL ❌'}")
    ok = ok and code == 409

    good = json.loads(json.dumps(BASE))
    good["fields"].append({"name": "note", "type": ["null", "string"], "default": None})
    code = register(PROBE, good)
    print(f"      schema tương thích (thêm optional) -> "
          f"{code} {'CHẤP NHẬN ✅' if code == 200 else 'FAIL ❌'}")
    ok = ok and code == 200

    _req("DELETE", f"/subjects/{PROBE}")  # dọn subject demo
    print(f"      đã xoá subject demo")

    print("\nSCHEMA COMPATIBILITY ENFORCED ✅" if ok else "\nENFORCEMENT SAI ❌")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
