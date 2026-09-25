"""Thư viện phân tích: trả lời "VÌ SAO đổi", không chỉ "đổi bao nhiêu".

Code ở đây do tôi viết, nhưng PHƯƠNG PHÁP thì không phải tôi nghĩ ra:

  compare_periods    so kỳ có căn chỉnh thứ trong tuần, loại kỳ dở dang
  decompose_drivers  LMDI (Log Mean Divisia Index) — Ang, B.W., Energy Policy 2005

VÌ SAO LMDI CHỨ KHÔNG PHẢI PHÉP CHIA NGÂY THƠ
  Sản lượng = A × B × C. Khi cả ba cùng đổi, cách tính đóng góp kiểu "giữ hai cái
  cố định, đổi cái thứ ba" để lại một SỐ DƯ TƯƠNG TÁC: tổng ba đóng góp KHÔNG bằng
  mức thay đổi thật. Phần thừa thường bị vứt đi im lặng, và người đọc tưởng đã
  giải thích hết.
  LMDI cho số dư bằng 0 theo ĐỊNH NGHĨA. Chứng minh một dòng: đóng góp của x là
  L(V1,V0)·ln(x1/x0) với L là trung bình logarit; cộng lại theo x được
  L(V1,V0)·ln(V1/V0) = V1 − V0. Không còn gì rơi rớt.
  Hàm vẫn TRẢ VỀ `residual` để người đọc tự kiểm — một con số nói "tôi đã giải
  thích hết" mà không cho kiểm thì không đáng tin hơn con số không nói gì.

⚠️ THỨ DỄ NÓI DỐI NHẤT Ở ĐÂY
  Quét nhiều chiều × nhiều giá trị là hàng trăm phép so sánh; kiểu gì cũng có cái
  "bất thường" thuần do ngẫu nhiên (multiple comparisons). Vì vậy các hàm dưới
  đây KHÔNG tự kết luận — chúng trả số kèm cảnh báo, và để tầng trên quyết định
  có đáng nói không.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any

from layer import SemanticLayer
from query import run

# Ba nhân tử của phép phân rã sản lượng. Thứ tự có ý nghĩa: từ "bao nhiêu người"
# tới "mỗi người bao nhiêu lần" tới "mỗi lần bao nhiêu tiền" — đúng trình tự một
# người kinh doanh sẽ hỏi.
DRIVER_METRICS = ["active_accounts", "daily_txn_count", "daily_throughput"]


def _d(x: str | date) -> date:
    return x if isinstance(x, date) else datetime.strptime(x, "%Y-%m-%d").date()


def _days(a: str | date, b: str | date) -> list[date]:
    a, b = _d(a), _d(b)
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def _weekday_mix(a: str | date, b: str | date) -> dict[int, int]:
    mix: dict[int, int] = {}
    for d in _days(a, b):
        mix[d.weekday()] = mix.get(d.weekday(), 0) + 1
    return mix


def _log_mean(a: float, b: float) -> float | None:
    """Trung bình logarit L(a,b) = (a−b)/(ln a − ln b), với L(a,a) = a.

    Trả None khi một vế ≤ 0: logarit không xác định. Ang đề xuất thay 0 bằng một
    δ rất nhỏ, nhưng làm thế là BỊA ra một con số rồi trình bày như kết quả —
    thà nói thẳng "không phân rã được" còn hơn.
    """
    if a <= 0 or b <= 0:
        return None
    if math.isclose(a, b):
        return a
    return (a - b) / (math.log(a) - math.log(b))


def period_warnings(
    base_from: str, base_to: str, comp_from: str, comp_to: str,
    exclude_today: bool = True,
) -> list[str]:
    """Những chỗ phép so kỳ dễ nói dối, nêu ra thay vì âm thầm bỏ qua."""
    warns: list[str] = []
    b_days, c_days = _days(base_from, base_to), _days(comp_from, comp_to)

    if len(b_days) != len(c_days):
        warns.append(
            f"Hai kỳ dài khác nhau ({len(b_days)} ngày so với {len(c_days)} ngày) — "
            "so tổng sẽ lệch. Hãy so giá trị trung bình mỗi ngày.")

    if _weekday_mix(base_from, base_to) != _weekday_mix(comp_from, comp_to):
        # Cuối tuần sản lượng chỉ bằng ~65% ngày thường, nên một kỳ nhiều cuối
        # tuần hơn sẽ "giảm" mà chẳng có gì xảy ra cả.
        warns.append(
            "Hai kỳ có số ngày cuối tuần khác nhau — chênh lệch có thể chỉ là "
            "mùa vụ trong tuần, không phải thay đổi thật.")

    # Chu kỳ THÁNG: lương về, hoá đơn, kỳ thanh toán thẻ đều bám ngày trong tháng.
    # Hai kỳ phủ những phần khác nhau của tháng sẽ lệch mà chẳng có gì xảy ra.
    # Không mã hoá "ngày lương là mùng 1 và 15" — đó là hằng số của thế giới mô
    # phỏng, không phải của mọi ngân hàng. Chỉ nói rằng hai kỳ KHÁC NHAU về chỗ
    # đứng trong tháng, và để người đọc tự biết chu kỳ của họ.
    b_dom = {d.day for d in b_days}
    c_dom = {d.day for d in c_days}
    if b_dom != c_dom and len(b_dom & c_dom) < len(b_dom) / 2:
        # Đếm ngày trùng, KHÔNG in khoảng min–max: một kỳ vắt qua ranh giới tháng
        # (27/07–02/08) có tập ngày {27..31, 1, 2} nên min–max thành "1–31", vừa
        # sai nghĩa vừa làm người đọc mất tin vào cảnh báo.
        warns.append(
            f"Hai kỳ rơi vào những phần khác nhau của tháng (chỉ {len(b_dom & c_dom)}"
            f"/{len(b_dom)} ngày trùng vị trí) — chu kỳ lương và hoá đơn có thể "
            "giải thích phần lớn chênh lệch.")

    today = datetime.now().date()
    if exclude_today and (today in b_days or today in c_days):
        warns.append(
            f"Kỳ có chứa hôm nay ({today}) — ngày chưa đóng sổ nên sẽ luôn trông "
            "như tụt. Đã loại khỏi phép tính.")
    return warns


def _clip_today(date_to: str, exclude_today: bool) -> str:
    if not exclude_today:
        return date_to
    today = datetime.now().date()
    return min(_d(date_to), today - timedelta(days=1)).isoformat()


def diff_rows(base: dict[tuple, Any], comp: dict[tuple, Any]) -> list[dict[str, Any]]:
    """Ghép hai kỳ thành bảng chênh lệch, xếp theo độ lớn thay đổi.

    ⚠️ VẮNG MẶT NGHĨA LÀ 0, KHÔNG PHẢI "KHÔNG BIẾT". Mart có một dòng cho mỗi
    (ngày, nhóm) CÓ hoạt động; nhóm không xuất hiện ở một kỳ tức là kỳ đó nó
    không hoạt động.
    Bản đầu để None rồi xếp theo `abs(delta or 0)` — nên một merchant TẮT HẲN
    nhận điểm 0 và rơi xuống CUỐI bảng. Tool sinh ra để tìm sự cố mà lại bỏ sót
    đúng loại sự cố rõ nhất. Lỗi chỉ lộ ra vì có đáp án để đối chiếu.
    """
    rows = []
    for key in sorted(base.keys() | comp.keys()):
        bv, cv = base.get(key), comp.get(key)
        appeared, disappeared = bv is None, cv is None
        bv, cv = float(bv or 0.0), float(cv or 0.0)
        delta = cv - bv
        rows.append({
            "key": list(key),
            "base": bv, "comp": cv, "delta": delta,
            # Không có mẫu số thì KHÔNG có phần trăm. Bịa "+∞%" hay "+100%" cho
            # một nhóm mới xuất hiện là bịa ra một con số.
            "pct_change": (delta / bv) if bv else None,
            "appeared": appeared, "disappeared": disappeared,
        })
    # Xếp theo mức thay đổi TUYỆT ĐỐI: hỏi "cái gì đổi nhiều nhất" là hỏi độ lớn.
    # Xếp theo phần trăm thì một nhóm bé tí nhảy từ 2 lên 6 luôn đứng đầu và che
    # mất thứ thật sự đáng nhìn.
    rows.sort(key=lambda r: abs(r["delta"]), reverse=True)
    return rows


def compare_periods(
    layer: SemanticLayer,
    metric: str,
    base_from: str, base_to: str,
    comp_from: str, comp_to: str,
    group_by: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    exclude_today: bool = True,
    limit: int = 50,
) -> dict[str, Any]:
    """So MỘT metric giữa hai kỳ. `base` là kỳ trước, `comp` là kỳ đang xét."""
    warns = period_warnings(base_from, base_to, comp_from, comp_to, exclude_today)
    b_to = _clip_today(base_to, exclude_today)
    c_to = _clip_today(comp_to, exclude_today)
    group_by = group_by or []

    def fetch(f: str, t: str) -> tuple[dict[tuple, Any], Any, str]:
        r = run(layer, metric, group_by=group_by, filters=filters,
                date_from=f, date_to=t, limit=limit if group_by else 1)
        i = r["columns"].index(metric)
        keyed = {tuple(row[:len(group_by)]): row[i] for row in r["rows"]}
        return keyed, r["watermark"], r["sql"]

    base, wm_b, sql_b = fetch(base_from, b_to)
    comp, wm_c, sql_c = fetch(comp_from, c_to)

    if len(base) >= limit or len(comp) >= limit:
        warns.append(
            f"Kết quả chạm trần {limit} dòng — có thể còn nhóm khác bị cắt, và "
            "nhóm bị cắt sẽ bị hiểu nhầm là bằng 0. Hãy thu hẹp bộ lọc.")
    rows = diff_rows(base, comp)

    return {
        "metric": metric,
        "metric_label": layer.metrics[metric].label,
        "base_period": [base_from, b_to],
        "comp_period": [comp_from, c_to],
        "group_by": group_by,
        "rows": rows,
        "warnings": warns,
        "watermark": max(w for w in (wm_b, wm_c) if w is not None) if (wm_b or wm_c) else None,
        "sql": [sql_b, sql_c],
    }


def decompose_drivers(
    layer: SemanticLayer,
    base_from: str, base_to: str,
    comp_from: str, comp_to: str,
    exclude_today: bool = True,
) -> dict[str, Any]:
    """Phân rã thay đổi sản lượng TRUNG BÌNH MỖI NGÀY thành ba nhân tử (LMDI).

        lưu lượng/ngày = số khách hoạt động/ngày
                       × số giao dịch mỗi khách
                       × giá trị mỗi giao dịch

    ⚠️ "Số khách hoạt động/ngày" là TRUNG BÌNH của số khách từng ngày, KHÔNG phải
    số khách duy nhất trong cả kỳ — một người hoạt động 5 ngày được tính 5 lần.
    Cộng đếm-phân-biệt qua nhiều ngày không ra số người thật, nên đại lượng duy
    nhất đúng ở đây là trung bình theo ngày. Phân rã trên TRUNG BÌNH MỖI NGÀY còn
    xử lý luôn chuyện hai kỳ dài khác nhau.
    """
    warns = period_warnings(base_from, base_to, comp_from, comp_to, exclude_today)
    b_to = _clip_today(base_to, exclude_today)
    c_to = _clip_today(comp_to, exclude_today)

    def factors(f: str, t: str) -> tuple[dict[str, float], int, Any, str]:
        r = run(layer, DRIVER_METRICS, group_by=["full_date"],
                date_from=f, date_to=t, limit=400)
        idx = {m: r["columns"].index(m) for m in DRIVER_METRICS}
        n = len(r["rows"])
        if n == 0:
            return {}, 0, r["watermark"], r["sql"]
        tot = {m: sum(row[idx[m]] or 0 for row in r["rows"]) for m in DRIVER_METRICS}
        return tot, n, r["watermark"], r["sql"]

    b_tot, b_n, wm_b, sql_b = factors(base_from, b_to)
    c_tot, c_n, wm_c, sql_c = factors(comp_from, c_to)
    if not b_tot or not c_tot:
        return {"error": "một trong hai kỳ không có dữ liệu",
                "base_period": [base_from, b_to], "comp_period": [comp_from, c_to]}

    def split(tot: dict[str, float], n: int) -> dict[str, float]:
        acc, txn, amt = (tot["active_accounts"], tot["daily_txn_count"],
                         tot["daily_throughput"])
        return {
            "active_accounts": acc / n,          # khách hoạt động mỗi ngày
            "txn_per_account": txn / acc if acc else 0.0,
            "value_per_txn": amt / txn if txn else 0.0,
            "_volume_per_day": amt / n,
        }

    b, c = split(b_tot, b_n), split(c_tot, c_n)
    v0, v1 = b["_volume_per_day"], c["_volume_per_day"]
    lm = _log_mean(v1, v0)

    names = ["active_accounts", "txn_per_account", "value_per_txn"]
    labels = {
        "active_accounts": "Số khách hoạt động mỗi ngày",
        "txn_per_account": "Số giao dịch mỗi khách",
        "value_per_txn": "Giá trị mỗi giao dịch",
    }
    if lm is None:
        return {"error": "không phân rã được: sản lượng một kỳ bằng 0 hoặc âm",
                "base": b, "comp": c, "warnings": warns}

    drivers = []
    for k in names:
        x0, x1 = b[k], c[k]
        contrib = lm * math.log(x1 / x0) if (x0 > 0 and x1 > 0) else None
        drivers.append({
            "factor": k, "label": labels[k],
            "base": x0, "comp": x1,
            "pct_change": (x1 - x0) / x0 if x0 else None,
            "contribution": contrib,
            "share_of_change": (contrib / (v1 - v0))
            if (contrib is not None and not math.isclose(v1, v0)) else None,
        })
    drivers.sort(key=lambda d: abs(d["contribution"] or 0), reverse=True)

    explained = sum(d["contribution"] or 0 for d in drivers)
    return {
        "volume_metric": "daily_throughput",
        "base_period": [base_from, b_to], "comp_period": [comp_from, c_to],
        "base_per_day": v0, "comp_per_day": v1,
        "change_per_day": v1 - v0,
        "drivers": drivers,
        # LMDI cho số dư bằng 0 theo định nghĩa. In ra để người đọc TỰ KIỂM —
        # nếu một ngày nào đó nó khác 0 thì công thức đã bị sửa hỏng.
        "residual": (v1 - v0) - explained,
        "warnings": warns,
        "watermark": max(w for w in (wm_b, wm_c) if w is not None) if (wm_b or wm_c) else None,
        "sql": [sql_b, sql_c],
    }


# =============================================================================
# TẬP TRUNG & QUY TRÁCH: "một thủ phạm hay cả thị trường?"
#
# HHI (Herfindahl–Hirschman Index) không phải thứ tôi nghĩ ra: nó nằm trong
# *Horizontal Merger Guidelines* của DOJ/FTC Mỹ, cơ quan chống độc quyền dùng nó
# để quyết định có chặn một thương vụ sáp nhập hay không. Thang 0–10.000:
#     < 1.500  phân tán   ·   1.500–2.500  tập trung vừa   ·   > 2.500  tập trung cao
# =============================================================================
HHI_MODERATE = 1_500
HHI_HIGH = 2_500

# Trần an toàn khi cần LẤY HẾT thực thể. HHI tính trên tập bị cắt sẽ bị THỔI LÊN
# (mất đuôi nhỏ => các phần còn lại trông lớn hơn), nên ở đây thà lấy dư còn hơn.
CONCENTRATION_LIMIT = 5_000


def herfindahl(values: list[float]) -> float:
    """HHI trên thang 0–10.000 của DOJ/FTC.

    Độc quyền tuyệt đối = 10.000. N đối thủ đều nhau = 10.000/N — nên 10.000/HHI
    đọc được là "số đối thủ đều nhau tương đương", thang trực giác hơn HHI thô.
    """
    total = sum(values)
    if total <= 0:
        return 0.0
    return sum(((v / total) * 100) ** 2 for v in values)


def _require_additive(layer: SemanticLayer, metric: str, tool: str) -> dict[str, Any] | None:
    """Chặn metric kiểu TỶ LỆ ở những tool chỉ đúng với số CỘNG ĐƯỢC.

    `explain_change` cộng hiệu của từng nhóm để ra hiệu tổng — đúng với doanh số,
    SAI với tỷ lệ: tỷ lệ thành công của 5 kênh cộng lại không phải tỷ lệ của cả hệ
    thống. `find_concentration` chia tỷ trọng — "BRANCH chiếm 22% tỷ lệ thành
    công" là một câu không có nghĩa. Cả hai vẫn ra một con số trông hợp lý, nên
    không chặn ở đây thì không ai phát hiện.
    Trả hướng dẫn thay vì ném lỗi: agent đọc được và tự chuyển sang tool đúng.
    """
    if metric not in layer.metrics:
        return {"error": f"metric '{metric}' không có",
                "hint": f"các metric: {', '.join(sorted(layer.metrics))}"}
    if layer.metrics[metric].kind == "ratio":
        return {"error": f"{tool} chỉ dùng được với metric CỘNG ĐƯỢC; '{metric}' là tỷ lệ",
                "hint": ("với tỷ lệ, dùng detect_divergence — nó gộp nhóm đối chứng "
                         "bằng tổng/tổng thay vì cộng các tỷ lệ")}
    return None


def find_concentration(
    layer: SemanticLayer,
    metric: str,
    dimension: str,
    date_from: str | None = None,
    date_to: str | None = None,
    filters: dict[str, Any] | None = None,
    top_n: int = 5,
) -> dict[str, Any]:
    """Đo mức tập trung của một metric trên một chiều.

    Trả về HHI, tỷ trọng top-N, và SỐ THỰC THỂ TƯƠNG ĐƯƠNG (10.000/HHI) — con số
    cuối là thứ nói được với người không rành kỹ thuật: "tập trung tương đương 46
    đối tác đều nhau", dễ hình dung hơn hẳn "HHI = 217".
    """
    bad = _require_additive(layer, metric, "find_concentration")
    if bad:
        return bad
    r = run(layer, metric, group_by=[dimension], filters=filters,
            date_from=date_from, date_to=date_to, limit=CONCENTRATION_LIMIT)
    i = r["columns"].index(metric)
    pairs = [(row[0], float(row[i])) for row in r["rows"] if row[i] and row[i] > 0]

    warns: list[str] = []
    if len(r["rows"]) >= CONCENTRATION_LIMIT:
        warns.append(
            f"Chạm trần {CONCENTRATION_LIMIT} thực thể — HHI sẽ bị THỔI LÊN vì "
            "mất phần đuôi nhỏ. Hãy thu hẹp bộ lọc.")
    dropped = len(r["rows"]) - len(pairs)
    if dropped:
        warns.append(f"Bỏ qua {dropped} nhóm có giá trị ≤ 0 (HHI cần tỷ trọng dương).")
    if not pairs:
        return {"error": "không có dữ liệu dương để tính tập trung", "warnings": warns}

    total = sum(v for _, v in pairs)
    pairs.sort(key=lambda p: p[1], reverse=True)
    shares = [(k, v, v / total) for k, v in pairs]
    hhi = herfindahl([v for _, v in pairs])

    level = ("cao" if hhi > HHI_HIGH else "vừa" if hhi > HHI_MODERATE else "phân tán")
    cum = 0.0
    top = []
    for k, v, s in shares[:top_n]:
        cum += s
        top.append({"key": k, "value": v, "share": s, "cumulative_share": cum})

    return {
        "metric": metric, "metric_label": layer.metrics[metric].label,
        "dimension": dimension,
        "period": [date_from, date_to],
        "entities": len(shares),
        "total": total,
        "hhi": hhi,
        "concentration": level,
        # 10.000/HHI = số đối tác ĐỀU NHAU cho ra cùng mức tập trung. Thang trực
        # giác hơn HHI thô, và không cần giải thích công thức cho người đọc.
        "effective_entities": 10_000 / hhi if hhi else None,
        "top": top,
        "top_n_share": cum,
        "warnings": warns,
        "watermark": r["watermark"],
        "sql": r["sql"],
    }


def explain_change(
    layer: SemanticLayer,
    metric: str,
    dimension: str,
    base_from: str, base_to: str,
    comp_from: str, comp_to: str,
    filters: dict[str, Any] | None = None,
    exclude_today: bool = True,
    cover: float = 0.80,
    top_n: int = 5,
) -> dict[str, Any]:
    """Ai gây ra mức thay đổi — và bao nhiêu kẻ mới giải thích hết phần lớn nó.

    Phép cộng này CHÍNH XÁC theo cấu trúc: tổng thay đổi bằng tổng các thay đổi
    thành phần, không cần LMDI (LMDI dành cho phân rã NHÂN, còn đây là phân rã
    CỘNG trên cùng một grain).

    ⚠️ BẪY LỚN NHẤT: các chiều BÙ TRỪ nhau. Doanh số có thể đứng yên trong khi
    một nửa thị trường sập và nửa kia tăng gấp đôi. Lúc đó "thay đổi thuần" gần 0
    và mọi `share_of_change` phóng lên vô nghĩa. Vì vậy hàm luôn trả
    `gross_movement` (tổng trị tuyệt đối) và cảnh báo khi nó lớn hơn hẳn thay đổi
    thuần — đó thường là phát hiện ĐÁNG GIÁ NHẤT, không phải một phiền toái.
    """
    bad = _require_additive(layer, metric, "explain_change")
    if bad:
        return bad
    warns = period_warnings(base_from, base_to, comp_from, comp_to, exclude_today)
    b_to = _clip_today(base_to, exclude_today)
    c_to = _clip_today(comp_to, exclude_today)

    def fetch(f: str, t: str):
        r = run(layer, metric, group_by=[dimension], filters=filters,
                date_from=f, date_to=t, limit=CONCENTRATION_LIMIT)
        i = r["columns"].index(metric)
        return {(row[0],): row[i] for row in r["rows"]}, r["watermark"], r["sql"]

    base, wm_b, sql_b = fetch(base_from, b_to)
    comp, wm_c, sql_c = fetch(comp_from, c_to)
    if len(base) >= CONCENTRATION_LIMIT or len(comp) >= CONCENTRATION_LIMIT:
        warns.append(
            f"Chạm trần {CONCENTRATION_LIMIT} thực thể — nhóm bị cắt sẽ bị hiểu "
            "nhầm là bằng 0.")

    rows = diff_rows(base, comp)
    net = sum(r["delta"] for r in rows)
    gross = sum(abs(r["delta"]) for r in rows)

    if gross > 0 and abs(net) < 0.5 * gross:
        warns.append(
            f"Các nhóm BÙ TRỪ nhau: tổng biến động {gross:,.0f} nhưng thay đổi "
            f"thuần chỉ {net:,.0f}. Con số tổng đang che giấu hai chiều ngược nhau "
            "— đây thường mới là điều đáng báo cáo.")

    # Bao nhiêu kẻ giải thích `cover` phần thay đổi. Chỉ có nghĩa khi các đóng góp
    # CÙNG CHIỀU với thay đổi thuần; ngược chiều thì bỏ ra khỏi phép đếm.
    same_way = [r for r in rows if net and (r["delta"] * net) > 0]
    same_way.sort(key=lambda r: abs(r["delta"]), reverse=True)
    n_to_cover, acc = None, 0.0
    for idx, r in enumerate(same_way, start=1):
        acc += abs(r["delta"])
        if abs(net) and acc >= cover * abs(net):
            n_to_cover = idx
            break

    top = []
    for r in rows[:top_n]:
        top.append({
            **r,
            # Tỷ trọng trên thay đổi THUẦN. Khó đọc khi ngược dấu: một merchant
            # tắt hẳn đóng góp -40% vào một mức tăng — đúng toán, rối mắt.
            "share_of_change": (r["delta"] / net) if net else None,
            # Tỷ trọng trên TỔNG BIẾN ĐỘNG. Luôn nằm trong 0..1 và đọc được kể
            # cả khi các chiều bù trừ nhau: "một merchant chiếm 22% toàn bộ
            # chuyển dịch" là câu ai cũng hiểu, không cần biết dấu.
            "share_of_gross": (abs(r["delta"]) / gross) if gross else None,
        })

    return {
        "metric": metric, "metric_label": layer.metrics[metric].label,
        "dimension": dimension,
        "base_period": [base_from, b_to], "comp_period": [comp_from, c_to],
        "net_change": net,
        "gross_movement": gross,
        "entities_explaining_cover": n_to_cover,
        "cover": cover,
        "top": top,
        "warnings": warns,
        "watermark": max(w for w in (wm_b, wm_c) if w is not None) if (wm_b or wm_c) else None,
        "sql": [sql_b, sql_c],
    }


# =============================================================================
# PHÂN KỲ: "cái này tụt, hay mọi thứ đều tụt?"
#
# Difference-in-differences — phương pháp chuẩn của kinh tế lượng. Ý tưởng: đừng
# hỏi "kênh ONLINE có tụt không", hãy hỏi "nó tụt HƠN phần còn lại bao nhiêu".
# Một đợt suy giảm toàn thị trường và một sự cố của riêng một kênh nhìn GIỐNG HỆT
# nhau nếu chỉ xem một đường; chỉ có nhóm đối chứng mới tách được hai thứ đó.
#
# ⚠️ NGƯỠNG BẰNG MEDIAN + MAD, KHÔNG PHẢI TRUNG BÌNH + ĐỘ LỆCH CHUẨN.
# Chính cái bất thường ta đang tìm sẽ kéo cả trung bình lẫn độ lệch chuẩn về phía
# nó, rồi tự làm mình trông bình thường. Median và MAD miễn nhiễm với chuyện đó.
# Ngưỡng 3,5 theo Iglewicz & Hoaglin.
# =============================================================================
ROBUST_Z_THRESHOLD = 3.5
# 0.6745 = Φ⁻¹(0,75): hằng số quy MAD về cùng thang với độ lệch chuẩn khi dữ liệu
# thật sự chuẩn, để ngưỡng 3,5 đọc được như "3,5 sigma".
MAD_TO_SIGMA = 0.6745


def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def robust_z(values: list[float]) -> list[float | None]:
    """Điểm lệch chuẩn hoá theo median + MAD.

    Trả None cho mọi phần tử khi MAD = 0 (quá nửa số điểm bằng nhau): lúc đó
    không có thang để đo lệch, và bịa ra một điểm là tệ hơn im lặng.
    """
    if len(values) < 3:
        return [None] * len(values)
    med = _median(values)
    mad = _median([abs(v - med) for v in values])
    if mad == 0:
        return [None] * len(values)
    return [MAD_TO_SIGMA * (v - med) / mad for v in values]


def _shift_month(day: str | date, months: int = 1) -> str:
    """Lùi một ngày về trước `months` tháng, GIỮ NGUYÊN ngày trong tháng.

    Dịch theo tháng lịch chứ không theo 28/30 ngày: thứ ta đang muốn khớp là VỊ
    TRÍ TRONG THÁNG (kỳ lương, kỳ hoá đơn), không phải thứ trong tuần.
    """
    d = _d(day)
    y, m = d.year, d.month - months
    while m <= 0:
        m += 12
        y -= 1
    # 31/03 lùi về tháng 2 -> kẹp về ngày cuối tháng đó.
    last = (date(y + (m == 12), (m % 12) + 1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last)).isoformat()


def detect_divergence(
    layer: SemanticLayer,
    metric: str,
    dimension: str,
    base_from: str, base_to: str,
    comp_from: str, comp_to: str,
    filters: dict[str, Any] | None = None,
    exclude_today: bool = True,
    limit: int = 200,
    placebo: bool = True,
) -> dict[str, Any]:
    """Nhóm nào đi lệch khỏi phần còn lại, và lệch có đáng kể không.

    Với mỗi giá trị của `dimension`:
        did = (thay đổi của NÓ) − (thay đổi của TẤT CẢ NHỮNG CÁI KHÁC)

    Với metric kiểu tỷ lệ, "tất cả những cái khác" tính bằng
    (tổng tử − tử của nó) / (tổng mẫu − mẫu của nó). KHÔNG phải trung bình các tỷ
    lệ thành viên — phép đó cho trọng số bằng nhau cho một kênh 10 giao dịch và
    một kênh 10 triệu giao dịch.
    """
    warns = period_warnings(base_from, base_to, comp_from, comp_to, exclude_today)
    b_to = _clip_today(base_to, exclude_today)
    c_to = _clip_today(comp_to, exclude_today)
    m = layer.metrics[metric]
    is_ratio = m.kind == "ratio"

    def fetch(f: str, t: str):
        r = run(layer, metric, group_by=[dimension], filters=filters,
                date_from=f, date_to=t, limit=limit, include_parts=is_ratio)
        cols = r["columns"]
        out = {}
        for row in r["rows"]:
            rec = {"value": row[cols.index(metric)]}
            if is_ratio:
                rec["num"] = row[cols.index(f"{metric}__num")]
                rec["den"] = row[cols.index(f"{metric}__den")]
            out[row[0]] = rec
        return out, r["watermark"], r["sql"]

    base, wm_b, sql_b = fetch(base_from, b_to)
    comp, wm_c, sql_c = fetch(comp_from, c_to)
    keys = sorted(set(base) | set(comp))
    if not keys:
        return {"error": "không có dữ liệu", "warnings": warns}

    def rest_value(pool: dict[str, dict], skip: str) -> float | None:
        """Giá trị của PHẦN CÒN LẠI, gộp đúng cách."""
        others = [v for k, v in pool.items() if k != skip]
        if not others:
            return None
        if is_ratio:
            num = sum(o["num"] or 0 for o in others)
            den = sum(o["den"] or 0 for o in others)
            return (num / den) if den else None
        return sum(o["value"] or 0 for o in others)

    rows = []
    for k in keys:
        bv = (base.get(k) or {}).get("value")
        cv = (comp.get(k) or {}).get("value")
        rb, rc = rest_value(base, k), rest_value(comp, k)
        own = None if (bv is None or cv is None) else cv - bv
        ctl = None if (rb is None or rc is None) else rc - rb
        rows.append({
            "key": k,
            "base": bv, "comp": cv, "own_change": own,
            # Phần trăm do CODE tính. Không có nó, mô hình tự chia rồi viết "−19,9%"
            # — con số đúng nhưng không truy được về kết quả tool nào (eval 25-09).
            # Chỉ cho metric cộng được: với tỷ lệ, "phần trăm của phần trăm" dễ bị
            # đọc nhầm thành điểm phần trăm, nên ở đó chỉ giữ `own_change`.
            "own_pct_change": (own / bv) if (not is_ratio and own is not None and bv)
            else None,
            "control_base": rb, "control_comp": rc, "control_change": ctl,
            "did": None if (own is None or ctl is None) else own - ctl,
        })

    dids = [r["did"] for r in rows if r["did"] is not None]
    zs = robust_z(dids)
    zi = iter(zs)
    for r in rows:
        r["robust_z"] = next(zi) if r["did"] is not None else None
        r["significant"] = (
            r["robust_z"] is not None and abs(r["robust_z"]) > ROBUST_Z_THRESHOLD)
    rows.sort(key=lambda r: abs(r["did"] or 0), reverse=True)

    if all(r["robust_z"] is None for r in rows):
        warns.append(
            "Không chấm được mức đáng kể (quá ít nhóm, hoặc hơn nửa số nhóm lệch "
            "y hệt nhau). Các con số dưới đây là chênh lệch thô.")
    # Quét nhiều nhóm là nhiều phép so sánh cùng lúc; nhắc thẳng thay vì để người
    # đọc tưởng mỗi con số đứng một mình.
    if len(rows) > 10 and not any(r["significant"] for r in rows):
        warns.append(
            f"Đã so {len(rows)} nhóm mà không nhóm nào vượt ngưỡng — nhiều khả "
            "năng không có gì bất thường, chỉ là dao động bình thường.")

    # ---- KIỂM ĐỊNH GIẢ DƯỢC ------------------------------------------------
    # DiD giả định "xu hướng song song": nếu không có sự cố thì các nhóm đi cùng
    # nhịp. Giả định đó VỠ khi một nhóm có nhịp riêng vì lý do cấu trúc — ví dụ
    # lương cả ngân hàng đổ vào kênh BRANCH đúng ngày mùng 1. Lúc đó BRANCH lệch
    # thật, DiD đúng về toán, nhưng kết luận "sự cố" là SAI.
    # Cách xử lý chuẩn: chạy lại đúng phép so đó ở MỘT CHU KỲ TRƯỚC. Nhóm nào
    # lệch y hệt trong cửa sổ giả dược thì đó là NHỊP, không phải sự cố.
    # Đây không phải giả thuyết: agent đã kết luận sai đúng ca này (23-09), sau
    # khi tự đọc cảnh báo chu kỳ tháng rồi lập luận vượt qua nó.
    flagged = [r for r in rows if r["significant"]]
    if placebo and flagged:
        try:
            pb = detect_divergence(
                layer, metric, dimension,
                _shift_month(base_from), _shift_month(b_to),
                _shift_month(comp_from), _shift_month(c_to),
                filters=filters, exclude_today=exclude_today, limit=limit,
                placebo=False)
            # So THAY ĐỔI CỦA CHÍNH NÓ, không so DiD.
            # DiD của cửa sổ giả dược đi qua nhóm đối chứng, nên một sự cố khác
            # nằm trong cửa sổ đó sẽ làm méo kết quả — đã dính thật: cửa sổ giả
            # dược tháng 8 trùng đợt hỏng kênh ONLINE, khiến DiD của BRANCH đổi
            # dấu và kiểm định vô hiệu. Câu hỏi "nhóm này có nhịp riêng không"
            # là câu hỏi VỀ CHÍNH NÓ và không cần nhóm đối chứng nào cả.
            pown = {r["key"]: r["own_change"] for r in pb.get("rows", [])}
            for r in rows:
                if not r["significant"]:
                    continue
                q = pown.get(r["key"])
                own = r["own_change"]
                r["placebo_own_change"] = q
                # Cùng dấu và ít nhất nửa độ lớn ở chu kỳ trước => là NHỊP.
                r["seasonal"] = (
                    q is not None and own is not None and q * own > 0
                    and abs(q) >= 0.5 * abs(own))
                if r["seasonal"]:
                    r["significant"] = False
                    warns.append(
                        f"'{r['key']}' đã đổi y hệt ở cùng kỳ tháng trước "
                        f"({q:+.4g} so với {own:+.4g}) — đây là NHỊP THEO CHU KỲ, "
                        "không phải sự cố mới. Đã bỏ cờ bất thường.")
                else:
                    # Nói cả chiều NGƯỢC LẠI. Thiếu câu này, agent thấy cảnh báo chu
                    # kỳ tháng rồi lấp lửng "chưa kết luận được" ngay cả khi nhóm đã
                    # qua kiểm định (eval 25-09, ca CORPORATE). Câu trả lời đúng mà
                    # không dám nói là câu trả lời vô dụng — và agent KHÔNG được tự
                    # lập luận vượt qua cảnh báo, nên căn cứ phải đến từ tool.
                    warns.append(
                        f"Đã kiểm định giả dược cho '{r['key']}': cùng kỳ tháng trước nó "
                        f"chỉ đổi {q if q is not None else 0:+.4g} (nay {own:+.4g}) — "
                        "chu kỳ tháng KHÔNG giải thích được mức lệch này.")
        except Exception as exc:  # noqa: BLE001
            warns.append(f"Không chạy được kiểm định giả dược: {exc}")

    return {
        "metric": metric, "metric_label": m.label, "dimension": dimension,
        "is_ratio": is_ratio,
        "base_period": [base_from, b_to], "comp_period": [comp_from, c_to],
        "rows": rows,
        # Để mô hình trích "đã so 5 kênh" từ tool thay vì tự đếm (eval 25-09).
        "groups_compared": len(rows),
        "placebo_checked": bool(placebo and flagged),
        "threshold": ROBUST_Z_THRESHOLD,
        "warnings": warns,
        "watermark": max(w for w in (wm_b, wm_c) if w is not None) if (wm_b or wm_c) else None,
        "sql": [sql_b, sql_c],
    }


# =============================================================================
# KHOẢNG IM LẶNG: "ai ngừng hoạt động, từ ngày nào tới ngày nào?"
#
# VÌ SAO CẦN TOOL RIÊNG (eval 25-09):
#   Hỏi "giữa tháng 7 có merchant nào ngừng giao dịch không", agent so 1–15/07 với
#   16–31/07. Sự cố thật nằm ở 10–17/07 — VẮT NGANG điểm chia: 6 ngày tắt rơi vào
#   kỳ đầu, 2 ngày vào kỳ sau. Kết quả: merchant tắt hẳn 8 ngày được báo là TĂNG
#   +33 triệu. Không phải bỏ sót — mà báo NGƯỢC.
#   Mọi tool so kỳ đều GỘP theo kỳ, nên một khoảng im lặng cắt ngang ranh giới kỳ
#   sẽ bị pha loãng hoặc đảo dấu. Câu hỏi này có hình dạng khác: nó là một LỖ
#   trong chuỗi ngày, và phải nhìn chuỗi ngày mới thấy.
#
# Phương pháp: phát hiện mất tín hiệu (heartbeat) — kỹ thuật chuẩn của giám sát
# hệ thống. Không có gì mới, chỉ là trước đây thư viện thiếu nó.
# =============================================================================
DROPOUT_CHUNK_DAYS = 30
# Thực thể mà ngày thường cũng hay im lặng thì một khoảng im lặng không nói lên
# gì. Quá ngưỡng này thì bỏ qua — và ĐẾM số bị bỏ qua, không im lặng bỏ.
SPARSE_ZERO_RATE = 0.20


def zero_runs(values: list[float], min_len: int) -> list[tuple[int, int]]:
    """Các đoạn liên tiếp bằng 0, dài ít nhất `min_len`. Trả (đầu, cuối) — tính cả hai."""
    runs, start = [], None
    for i, v in enumerate(values + [1.0]):          # lính canh đóng đoạn cuối
        if v == 0 and start is None:
            start = i
        elif v != 0 and start is not None:
            if i - start >= min_len:
                runs.append((start, i - 1))
            start = None
    return [(a, b) for a, b in runs if b < len(values)]


def dropouts_from_series(
    series: dict[str, dict[date, float]],
    days: list[date],
    min_gap_days: int = 2,
) -> tuple[list[dict[str, Any]], int]:
    """Phần thuần logic của find_dropouts (CI test được, không cần DB).

    Trả (danh sách khoảng im lặng, số thực thể bị bỏ qua vì vốn thưa thớt).
    """
    found, skipped_sparse = [], 0
    for ent, byday in series.items():
        vals = [float(byday.get(d, 0) or 0) for d in days]
        first = next((i for i, v in enumerate(vals) if v > 0), None)
        if first is None:
            continue
        # Số 0 TRƯỚC ngày hoạt động đầu tiên là "chưa tồn tại", không phải "tắt".
        span = vals[first:]
        runs = [(a + first, b + first) for a, b in zero_runs(span, min_gap_days)]
        if not runs:
            continue
        kept_any, dropped_any = False, False
        for a, b in runs:
            # ĐO THƯA THỚT RIÊNG CHO TỪNG KHOẢNG: "bỏ khoảng này ra, thực thể này có
            # hay im lặng không?". Bản đầu đo trên phần NGOÀI MỌI khoảng — và bị
            # vòng tròn: thực thể im lặng 2/3 số ngày theo từng đợt đúng 2 ngày thì
            # mọi số 0 đều bị hút vào các khoảng, phần còn lại toàn ngày có hoạt
            # động, tỷ lệ im lặng ra 0% -> báo 6 sự cố giả. Unit test bắt được.
            outside = [v for i, v in enumerate(vals) if i >= first and not a <= i <= b]
            if not outside:
                continue
            zero_rate = sum(1 for v in outside if v == 0) / len(outside)
            if zero_rate > SPARSE_ZERO_RATE:
                dropped_any = True
                continue
            kept_any = True
            baseline = _median(sorted(v for v in outside if v > 0))
            n = b - a + 1
            found.append({
                "key": ent,
                "start": days[a].isoformat(), "end": days[b].isoformat(),
                "days": n,
                "ongoing": b == len(days) - 1,
                "baseline_per_day": baseline,
                # Ước lượng khối lượng mất đi = mức thường ngày × số ngày im lặng.
                # Xếp hạng theo con số này: merchant tí hon im lặng 3 ngày không
                # quan trọng bằng merchant lớn nhất im lặng 1 ngày.
                "estimated_lost": baseline * n,
            })
        if dropped_any and not kept_any:
            skipped_sparse += 1
    found.sort(key=lambda r: r["estimated_lost"], reverse=True)
    return found, skipped_sparse


def find_dropouts(
    layer: SemanticLayer,
    metric: str,
    dimension: str,
    date_from: str,
    date_to: str,
    filters: dict[str, Any] | None = None,
    min_gap_days: int = 2,
    exclude_today: bool = True,
    top_n: int = 10,
) -> dict[str, Any]:
    """Thực thể nào IM LẶNG liên tiếp ≥ `min_gap_days` ngày trong khi thường ngày vẫn hoạt động."""
    bad = _require_additive(layer, metric, "find_dropouts")
    if bad:
        return bad
    to = _clip_today(date_to, exclude_today)
    days = _days(date_from, to)
    if not days:
        return {"error": "khoảng ngày rỗng"}

    series: dict[str, dict[date, float]] = {}
    wms, sqls, warns = [], [], []
    from layer import MAX_LIMIT
    # Lấy theo từng khúc 30 ngày: một truy vấn 90 ngày × 200 merchant vượt trần
    # 10.000 dòng. Và ở tool NÀY, bị cắt dòng không chỉ là thiếu dữ liệu — dòng bị
    # cắt trông y hệt một ngày im lặng, tức là sinh ra sự cố GIẢ.
    for i in range(0, len(days), DROPOUT_CHUNK_DAYS):
        chunk = days[i:i + DROPOUT_CHUNK_DAYS]
        r = run(layer, metric, group_by=["full_date", dimension], filters=filters,
                date_from=chunk[0].isoformat(), date_to=chunk[-1].isoformat(),
                limit=MAX_LIMIT)
        if len(r["rows"]) >= MAX_LIMIT:
            return {"error": (f"chạm trần {MAX_LIMIT} dòng — dòng bị cắt sẽ trông như "
                              "ngày im lặng và sinh sự cố GIẢ. Thu hẹp bộ lọc.")}
        ci = r["columns"].index(metric)
        for row in r["rows"]:
            d = row[0] if isinstance(row[0], date) else _d(str(row[0]))
            series.setdefault(row[1], {})[d] = row[ci]
        wms.append(r["watermark"])
        sqls.append(r["sql"])

    found, skipped = dropouts_from_series(series, days, min_gap_days)
    if skipped:
        warns.append(f"Bỏ qua {skipped} thực thể vốn thưa thớt (im lặng >"
                     f"{SPARSE_ZERO_RATE:.0%} số ngày) — với chúng, vài ngày im lặng không "
                     "nói lên điều gì.")
    return {
        "metric": metric, "metric_label": layer.metrics[metric].label,
        "dimension": dimension,
        "period": [date_from, to],
        "entities_scanned": len(series),
        "dropouts": found[:top_n],
        "dropouts_total": len(found),
        "warnings": warns,
        "watermark": max((w for w in wms if w is not None), default=None),
        "sql": sqls,
    }
