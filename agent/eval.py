"""Bộ eval tự động: chạy agent qua các sự cố đã cấy sẵn và CHẤM ĐIỂM.

VÌ SAO CẦN
  Trước đây agent được chấm bằng mắt, từng câu một. Mỗi lần sửa prompt hay sửa
  tool là không biết có làm hỏng câu khác không. Hai lỗi nghiêm trọng nhất tìm được
  (BRANCH "rơi 50,9%" do kỳ lương; bộ kiểm số không kiểm gì) đều lộ ra nhờ chấm —
  nên chấm phải lặp lại được, không phải việc làm một lần.

ĐÁP ÁN SUY RA TỪ DỮ LIỆU, KHÔNG GÕ TAY
  Sự cố cấy theo "N ngày trước lúc dựng thế giới". Mốc đó = ngày seed tài khoản,
  đọc từ `dim_account.opened_at`; cửa sổ lấy từ hằng số trong `faker/world.py`.
  Sau mỗi lần reset, eval tự chỉnh theo — không có ngày nào bị chôn cứng ở đây.
  Đọc qua `marts_ro`, cùng ranh giới quyền với chính agent.

HAI LOẠI CA, VÀ LOẠI THỨ HAI QUAN TRỌNG HƠN
  positive — có sự cố thật: agent phải TÌM RA đúng thủ phạm và đúng khoảng ngày.
  negative — giai đoạn yên tĩnh có BẪY chu kỳ lương: agent KHÔNG được bịa sự cố.
  Một agent báo động mọi thứ sẽ qua hết ca positive. Chỉ ca negative mới phân biệt
  được nó với một agent thật sự hiểu dữ liệu.

KHÔNG DÙNG LLM LÀM GIÁM KHẢO
  Chấm bằng dữ kiện có cấu trúc (tool nào được gọi, dòng nào bị gắn cờ, con số nào
  truy được) cộng kiểm tra chuỗi đơn giản. Giám khảo LLM thêm chi phí và thêm một
  nguồn không tái lập được — đúng thứ bộ eval phải loại bỏ.

Chạy:  ~/working/gtl-agent-venv/bin/python agent/eval.py [--only ID ...]
"""
from __future__ import annotations

import json
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "agent"))
sys.path.insert(0, str(ROOT / "semantic"))
sys.path.insert(0, str(ROOT / "faker"))

import psycopg2  # noqa: E402
import world  # noqa: E402
from query import dsn  # noqa: E402

RESULTS_DIR = ROOT / "agent" / "eval_results"


# ---------------------------------------------------------------------------
# ĐÁP ÁN
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AnswerKey:
    anchor: date
    outage_merchant: str
    w_outage: tuple[date, date]
    w_fraud: tuple[date, date]
    w_channel: tuple[date, date]
    decline_from: date


def load_answer_key() -> AnswerKey:
    with psycopg2.connect(dsn()) as conn, conn.cursor() as cur:
        cur.execute("select min(opened_at)::date from dim_account")
        anchor = cur.fetchone()[0]
        # Merchant bị tắt là merchant index 0 trong thứ tự faker nạp (ORDER BY
        # merchant_id) — tức merchant_id nhỏ nhất, cũng là merchant nặng nhất.
        cur.execute("select merchant_name from dim_merchant order by merchant_id limit 1")
        outage = cur.fetchone()[0]

    def win(w: tuple[int, int]) -> tuple[date, date]:
        start, end = w
        return anchor - timedelta(days=start), anchor - timedelta(days=end)

    return AnswerKey(
        anchor=anchor,
        outage_merchant=outage,
        w_outage=win(world.INCIDENT_MERCHANT_OUTAGE),
        w_fraud=win(world.INCIDENT_FRAUD_BURST),
        w_channel=win(world.INCIDENT_CHANNEL_DEGRADE),
        decline_from=anchor - timedelta(days=world.INCIDENT_AMOUNT_DECLINE_FROM),
    )


# ---------------------------------------------------------------------------
# ĐỌC NGÀY TRONG CÂU TRẢ LỜI
# ---------------------------------------------------------------------------
_RANGE = re.compile(r"(\d{1,2})\s*[–—-]\s*(\d{1,2})\s*/\s*(\d{1,2})")
_SINGLE = re.compile(r"(?<![\d/])(\d{1,2})\s*/\s*(\d{1,2})(?![\d/])")
_ISO = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
# "10/07/2026" — dạng agent hay viết nhất. Bản đầu thiếu nó nên một câu trả lời
# ĐÚNG TỪNG NGÀY (10/07/2026 → 17/07/2026) bị chấm là khoanh sai (eval 25-09).
_DMY = re.compile(r"(\d{1,2})\s*/\s*(\d{1,2})\s*/\s*(\d{4})")


def dates_in(text: str, year: int) -> set[date]:
    """Mọi ngày được nhắc tới: '23–27/07' nở thành 5 ngày, '15/07', '2026-07-15'."""
    out: set[date] = set()

    def add(y: int, m: int, d: int) -> None:
        try:
            out.add(date(y, m, d))
        except ValueError:
            pass

    for d, m, y in _DMY.findall(text):
        add(int(y), int(m), int(d))
    text = _DMY.sub(" ", text)
    for a, b, m in _RANGE.findall(text):
        for d in range(int(a), int(b) + 1):
            add(year, int(m), d)
    for d, m in _SINGLE.findall(_RANGE.sub(" ", text)):
        add(year, int(m), int(d))
    for y, m, d in _ISO.findall(text):
        add(int(y), int(m), int(d))
    return out


def overlaps(text: str, window: tuple[date, date], slack: int = 1) -> bool:
    """Câu trả lời có khoanh đúng khoảng ngày không (cho lệch `slack` ngày ở mép)."""
    lo, hi = window[0] - timedelta(days=slack), window[1] + timedelta(days=slack)
    return any(lo <= d <= hi for d in dates_in(text, window[0].year))


# ---------------------------------------------------------------------------
# TRA CỨU TRONG KẾT QUẢ TOOL
# ---------------------------------------------------------------------------
def _rows(fact: dict[str, Any]) -> list[dict[str, Any]]:
    rows = fact.get("rows") or fact.get("top") or []
    return [r for r in rows if isinstance(r, dict)]


def _key_str(r: dict[str, Any]) -> str:
    k = r.get("key")
    return " ".join(map(str, k)) if isinstance(k, list) else str(k)


def flagged(facts: list[dict[str, Any]]) -> list[str]:
    """Mọi nhóm bị detect_divergence gắn cờ đáng kể (sau kiểm định giả dược)."""
    return [
        f"{f.get('dimension')}={_key_str(r)}"
        for f in facts if f.get("tool") == "detect_divergence"
        for r in _rows(f) if r.get("significant")
    ]


# ---------------------------------------------------------------------------
# CA KIỂM TRA
# ---------------------------------------------------------------------------
Check = tuple[str, bool, str]


@dataclass
class Case:
    id: str
    kind: str                      # "positive" | "negative"
    question: str
    checks: Callable[[dict[str, Any], AnswerKey], list[Check]] = field(repr=False)


def _universal(r: dict[str, Any]) -> list[Check]:
    tools = r["tools_used"]
    v = r["verification"] or {}
    unv = v.get("unverified", [])
    return [
        ("gọi list_metrics trước", bool(tools) and tools[0] == "list_metrics",
         " -> ".join(tools[:3]) + (" ..." if len(tools) > 3 else "")),
        ("không có số bịa", not unv, f"chưa truy được: {unv}" if unv else "sạch"),
        ("nêu watermark", "offset" in r["answer"].lower(), ""),
    ]


def _c_outage(r, k):
    a = r["answer"]
    surname = k.outage_merchant.split(",")[0].split()[0]
    evidence = any(
        _key_str(row).startswith(surname) and (row.get("disappeared") or row.get("comp") == 0)
        for f in r["facts"] for row in _rows(f)) or any(
        str(d.get("key", "")).startswith(surname)
        for f in r["facts"] if f.get("tool") == "find_dropouts"
        for d in f.get("dropouts", []))
    return [
        ("chỉ đúng merchant", surname.lower() in a.lower(), k.outage_merchant),
        ("khoanh đúng khoảng ngày", overlaps(a, k.w_outage),
         f"{k.w_outage[0]:%d/%m}–{k.w_outage[1]:%d/%m}"),
        ("có dữ kiện: merchant biến mất", evidence, ""),
    ]


def _c_fraud(r, k):
    a = r["answer"]
    used = any(f.get("metric") == "fraud_cases" or "fraud_cases" in str(f.get("sql", ""))
               for f in r["facts"])
    return [
        ("khoanh đúng khoảng ngày", overlaps(a, k.w_fraud),
         f"{k.w_fraud[0]:%d/%m}–{k.w_fraud[1]:%d/%m}"),
        ("dùng số VỤ, không chỉ số tiền", used,
         "đợt dò thẻ chỉ lộ ra ở số vụ; tiền gần như không đổi"),
    ]


def _c_channel(r, k):
    a = r["answer"]
    fl = flagged(r["facts"])
    return [
        ("chỉ đúng kênh", world.DEGRADED_CHANNEL in a, world.DEGRADED_CHANNEL),
        ("có dữ kiện: DiD gắn cờ đúng kênh",
         any(x.endswith("=" + world.DEGRADED_CHANNEL) for x in fl), str(fl)),
        ("KHÔNG gắn cờ oan kênh khác",
         all(x.endswith("=" + world.DEGRADED_CHANNEL) for x in fl if x.startswith("channel=")),
         str(fl)),
        ("khoanh đúng khoảng ngày", overlaps(a, k.w_channel, slack=2),
         f"{k.w_channel[0]:%d/%m}–{k.w_channel[1]:%d/%m}"),
    ]


def _c_decline(r, k):
    a = r["answer"]
    p = world.DECLINING_PERSONA
    evidence = any(
        p in _key_str(row)
        and (row.get("significant") or f.get("tool") == "explain_change")
        for f in r["facts"] for row in _rows(f)[:3])
    return [
        ("chỉ đúng nhóm khách", p in a, p),
        ("có dữ kiện: nhóm đó đứng đầu / bị gắn cờ", evidence, ""),
    ]


def _c_quiet(r, k):
    fl = flagged(r["facts"])
    called = any(f.get("tool") == "detect_divergence" for f in r["facts"])
    return [
        ("có thật sự kiểm tra (gọi detect_divergence)", called,
         "không kiểm thì 'không có gì' chẳng chứng minh được gì"),
        ("KHÔNG bịa sự cố", not fl, f"bị gắn cờ: {fl}" if fl else "không cờ nào"),
    ]


CASES = [
    Case("s1_outage", "positive",
         "Giữa tháng 7 có merchant lớn nào đột ngột ngừng giao dịch không? "
         "Nếu có thì là ai và trong khoảng ngày nào?", _c_outage),
    Case("s2_fraud", "positive",
         "Tình hình gian lận cuối tháng 7 có gì đáng chú ý không?", _c_fraud),
    Case("s3_channel", "positive",
         "Tháng 8 các kênh giao dịch có gì bất thường không?", _c_channel),
    Case("s4_decline", "positive",
         "Giá trị trung bình mỗi giao dịch từ cuối tháng 8 giảm hẳn so với đầu tháng 8. "
         "Nhóm khách hàng nào gây ra?", _c_decline),
    # Hai ca "yên tĩnh" đều là BẪY: mỗi cặp tuần có đúng một bên chứa ngày lương,
    # nên kênh BRANCH (nơi lương đi qua) lệch rõ rệt mà không có sự cố nào cả.
    Case("n1_quiet_payday_base", "negative",
         "Tuần 08–14/09 so với tuần 01–07/09 có gì bất thường không?", _c_quiet),
    Case("n2_quiet_payday_comp", "negative",
         "Tuần 15–21/09 so với tuần 08–14/09 có gì bất thường không?", _c_quiet),
]


# ---------------------------------------------------------------------------
def main() -> int:
    only = set(sys.argv[sys.argv.index("--only") + 1:]) if "--only" in sys.argv else None
    from graph import ask  # nạp muộn: không cần LLM để in đáp án khi debug

    key = load_answer_key()
    print(f"mốc thế giới {key.anchor} · merchant tắt: {key.outage_merchant}")
    print(f"cửa sổ: tắt {key.w_outage[0]:%d/%m}–{key.w_outage[1]:%d/%m} · "
          f"dò thẻ {key.w_fraud[0]:%d/%m}–{key.w_fraud[1]:%d/%m} · "
          f"ONLINE {key.w_channel[0]:%d/%m}–{key.w_channel[1]:%d/%m} · "
          f"CORPORATE từ {key.decline_from:%d/%m}\n")

    results = []
    for case in CASES:
        if only and case.id not in only:
            continue
        t0 = time.time()
        try:
            r = ask(case.question)
            checks = _universal(r) + case.checks(r, key)
            err = None
        except Exception as exc:  # noqa: BLE001 — một ca hỏng không giết cả lượt
            r, checks, err = {"answer": "", "tools_used": []}, [], f"{type(exc).__name__}: {exc}"
        dt = time.time() - t0
        ok = err is None and all(c[1] for c in checks)
        results.append({"id": case.id, "kind": case.kind, "question": case.question,
                        "passed": ok, "seconds": round(dt, 1), "error": err,
                        "checks": [{"name": n, "ok": o, "detail": d} for n, o, d in checks],
                        "tools_used": r.get("tools_used", []), "answer": r.get("answer", "")})
        mark = "✅" if ok else "❌"
        print(f"{mark} {case.id:22s} {case.kind:8s} {dt:5.0f}s")
        if err:
            print(f"     LỖI: {err}")
        for n, o, d in checks:
            if not o:
                print(f"     ✗ {n}" + (f"  ({d})" if d else ""))

    passed = sum(r["passed"] for r in results)
    print(f"\nKẾT QUẢ: {passed}/{len(results)} ca đạt")
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{datetime.now():%Y%m%d-%H%M%S}.json"
    out.write_text(json.dumps({
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "anchor": key.anchor.isoformat(),
        "passed": passed, "total": len(results), "cases": results,
    }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"chi tiết: {out.relative_to(ROOT)}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
