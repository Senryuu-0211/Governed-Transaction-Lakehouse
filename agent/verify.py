"""Đối chiếu MỌI con số trong câu trả lời ngược về kết quả tool.

VÌ SAO CẦN
  Mô hình ngôn ngữ bịa số một cách rất thuyết phục — và với dữ liệu tài chính thì
  một con số bịa nguy hiểm hơn hẳn một câu văn vụng. Tầng semantic đã bảo đảm số
  LẤY RA là đúng; tầng này bảo đảm số ĐƯA VÀO CÂU TRẢ LỜI đúng là số đã lấy ra.
  Hai chuyện khác nhau, và chỉ có cái thứ hai là thứ người dùng đọc.

KHÔNG CHẶN, MÀ DÁN NHÃN
  Chặn cứng nghe nghiêm khắc hơn nhưng sai: mô hình có quyền tính "giảm 25 điểm
  phần trăm" từ 95,3 và 70,0 — đó là số HỢP LỆ tuy không nằm trong fact nào. Nên
  ở đây chia ba mức, và mức cuối được nêu THẲNG cho người đọc:
      verified  — khớp một giá trị trong kết quả tool
      derived   — khớp hiệu của hai giá trị trong kết quả tool
      unverified— không truy về đâu được  ->  nghi bịa, phải nói rõ

  Một cảnh báo người dùng ĐỌC ĐƯỢC còn có ích hơn một lần từ chối im lặng khiến
  họ hỏi lại cùng câu đó cho tới khi may mắn qua được.
"""
from __future__ import annotations

import re
from typing import Any

# Bắt cả hai quy ước: "1,234.56" (Anh) và "1.234,56" (Việt).
NUMBER_RE = re.compile(r"-?\d[\d.,]*\d|-?\d")

# NGÀY THÁNG KHÔNG PHẢI SỐ LIỆU. Xoá trước khi dò số, nếu không "08–14/09" sinh
# ra ba "con số" không truy về đâu được, và cảnh báo bịa-số bị nhiễu tới mức
# không ai đọc nữa — cảnh báo sai nhiều lần thì cảnh báo đúng cũng mất tác dụng.
DATE_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}"                       # 2026-09-14
    r"|\d{1,2}\s*[–—-]\s*\d{1,2}[/-]\d{1,2}"    # 08–14/09
    r"|\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?"      # 14/09, 14/09/2026
    r"|\(\s*\d{1,2}\s*[–—-]\s*\d{1,2}\s*\)"     # (1–15)  — khoảng ngày trong tháng
    r"|ngày\s+\d{1,2}(?:\s*[–—-]\s*\d{1,2})?"   # ngày 31, ngày 16–31
    r"|\bT\d{1,2}\b|tháng\s+\d{1,2}"            # T7, tháng 8
    r"|thứ\s+\d{1,2}",                           # cảnh báo thứ 3, Thứ 2 (thứ trong tuần)
    re.IGNORECASE,                                # "Thứ 2", "Ngày 15", "Tháng 8"
)

# SỐ THỨ TỰ CŨNG KHÔNG PHẢI SỐ LIỆU: "2. Hai kỳ dài khác nhau", "cảnh báo (3)".
# Lỗi đã xảy ra thật (eval 25-09): một câu trả lời ĐÚNG bị chấm trượt vì ba số
# thứ tự danh sách bị coi là số bịa. Bộ chấm oan người đúng thì không ai tin nó
# nữa — kể cả lúc nó bắt đúng kẻ bịa.
LIST_RE = re.compile(r"(?m)^\s*\d{1,2}[.)]\s+|\(\d{1,2}\)")

# Sai số tương đối: mô hình làm tròn khi trình bày ("13,4 tỷ" cho 13.402.070.330).
REL_TOL = 0.005
# ⚠️ PHẢI NHỎ. Bản đầu để 0.5 và điều đó làm hỏng chính bộ kiểm tra: với tỷ lệ
# (0..1) thì sai số 0,5 rộng hơn cả miền giá trị, nên MỌI tỷ lệ bịa đều "khớp".
# Bộ gác trở thành vô dụng ở đúng loại số hay bị bịa nhất, mà vẫn báo "sạch".
ABS_TOL = 0.005
MAX_PAIRS = 400          # trần cho phép thử hiệu hai số, tránh nổ tổ hợp


def _parse(tok: str) -> float | None:
    t = tok.strip().rstrip(".,")
    if not t or t in {"-", "."}:
        return None
    # "1.234,56" -> dấu phẩy là thập phân;  "1,234.56" -> dấu chấm là thập phân.
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    elif "," in t:
        frac = t.rsplit(",", 1)[1]
        t = t.replace(",", "." if len(frac) != 3 else "")
    elif t.count(".") >= 1:
        frac = t.rsplit(".", 1)[1]
        if len(frac) == 3 and t.count(".") >= 1 and len(t.split(".")[0]) <= 3:
            # "13.402" mơ hồ: vừa có thể là 13402 vừa có thể là 13,402.
            # Trả bản KHÔNG dấu phân cách; bản kia được thử riêng ở dưới.
            t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def _decimals(tok: str) -> int:
    """Số chữ số thập phân mà token TỰ KHAI — theo cùng quy tắc với _parse."""
    t = tok.strip().rstrip(".,").lstrip("-")
    if "," in t and "." in t:
        sep = "," if t.rfind(",") > t.rfind(".") else "."
        return len(t.rsplit(sep, 1)[1])
    if "," in t:
        frac = t.rsplit(",", 1)[1]
        return 0 if len(frac) == 3 else len(frac)
    if "." in t:
        frac = t.rsplit(".", 1)[1]
        return 0 if (len(frac) == 3 and len(t.split(".")[0]) <= 3) else len(frac)
    return 0


def collect_numbers(obj: Any, out: list[float] | None = None) -> list[float]:
    """Mọi số xuất hiện trong kết quả tool, duyệt sâu."""
    out = [] if out is None else out
    if isinstance(obj, bool):
        return out
    if isinstance(obj, (int, float)):
        out.append(float(obj))
    elif isinstance(obj, str):
        # Cảnh báo của tool viết bằng chữ ("15 ngày so với 16 ngày") — mô hình trích
        # lại là trích NGUỒN, không phải bịa. Bỏ qua chúng là bắt oan.
        for tok in NUMBER_RE.findall(DATE_RE.sub(" ", obj)):
            val = _parse(tok)
            if val is not None:
                out.append(val)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            # SQL/params chứa số của chính CÂU LỆNH (LIMIT 1000...); fact_id là mã
            # nội bộ ("F12"). Gộp chúng vào kho là tạo chỗ trú cho số bịa.
            if k in ("sql", "params", "fact_id"):
                continue
            collect_numbers(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            collect_numbers(v, out)
    return out


def _close(a: float, b: float, tol: float = ABS_TOL) -> bool:
    """So theo ĐỘ LỚN, bỏ qua dấu.

    Dấu do CHỮ mang: "mất 54 triệu" và "-54 triệu" là cùng một sự thật. Bắt bẻ
    dấu ở đây chỉ đẩy những câu viết đúng vào nhóm "chưa truy được", và một cảnh
    báo sai nhiều lần là cảnh báo không ai đọc nữa.
    """
    a, b = abs(a), abs(b)
    return abs(a - b) <= max(tol, REL_TOL * max(a, b))


SCALES = (1.0, 100.0, 0.01, 1e6, 1e9)


def _variants(x: float) -> list[tuple[float, float]]:
    """Cùng một đại lượng, khác cách trình bày — kèm HỆ SỐ để co giãn sai số theo.

    Tỷ lệ nằm trong fact dưới dạng 0,953 còn câu trả lời viết "95,3%"; tiền để
    nguyên đồng nhưng câu trả lời viết "13,4 tỷ". "13,4 tỷ" tự khai ±0,05 tỷ, tức
    ±50 triệu đồng — sai số phải phóng theo đúng hệ số đó.
    """
    return [(x * m, m) for m in SCALES]


def verify_answer(text: str, facts: list[dict[str, Any]]) -> dict[str, Any]:
    """Phân loại từng con số trong câu trả lời."""
    pool: list[float] = []
    for f in facts:
        collect_numbers(f, pool)
    pool = [p for p in pool if p is not None]

    # Hiệu của hai giá trị — mô hình được phép tự trừ để nói "giảm 25,2 điểm".
    diffs: list[float] = []
    if len(pool) <= MAX_PAIRS:
        seen = set()
        for i, a in enumerate(pool):
            for b in pool[i + 1:]:
                d = round(abs(a - b), 6)
                if d and d not in seen:
                    seen.add(d)
                    diffs.append(d)

    checked: list[dict[str, Any]] = []
    for tok in NUMBER_RE.findall(LIST_RE.sub(" ", DATE_RE.sub(" ", text))):
        val = _parse(tok)
        if val is None:
            continue
        # Năm tháng và số thứ tự nhỏ không phải "số liệu" — dán nhãn riêng thay
        # vì bắt oan, nhưng KHÔNG im lặng bỏ qua.
        if 1900 <= val <= 2100 and float(val).is_integer():
            checked.append({"token": tok, "value": val, "status": "date_like"})
            continue
        # SAI SỐ SUY RA TỪ ĐỘ CHÍNH XÁC MÔ HÌNH TỰ VIẾT (eval 25-09).
        # Một ngưỡng cố định không thoả được cả hai loại số: tỷ lệ cần CHẶT (0,005 —
        # ngưỡng 0,5 từng để mọi tỷ lệ bịa lọt qua), còn số nguyên làm tròn cần RỘNG
        # (agent viết "−8 USD" cho −7,6 và bị chấm là bịa). Viết "8" là tự nhận ±0,5;
        # viết "0,42" là tự nhận ±0,005. Chấm theo đúng lời tự nhận đó.
        half_unit = 0.5 * 10 ** (-_decimals(tok))
        status = "unverified"
        for cand, m in _variants(val):
            if any(_close(cand, p, half_unit * m) for p in pool):
                status = "verified"
                break
        if status == "unverified":
            for cand, m in _variants(val):
                if any(_close(cand, d, half_unit * m) for d in diffs):
                    status = "derived"
                    break
        checked.append({"token": tok, "value": val, "status": status})

    unverified = [c for c in checked if c["status"] == "unverified"]
    return {
        "numbers": checked,
        "verified": sum(1 for c in checked if c["status"] == "verified"),
        "derived": sum(1 for c in checked if c["status"] == "derived"),
        "unverified": [c["token"] for c in unverified],
        "clean": not unverified,
    }
