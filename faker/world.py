"""Thế giới mô phỏng của banking source — LOGIC THUẦN, không chạm DB.

Tách khỏi `generate_transactions.py` vì hai lý do:
  1. Đây là nơi chứa mọi giả định về "thị trường trông như thế nào". Trộn nó với
     code ghi DB thì mỗi lần muốn chỉnh hành vi lại phải đọc qua psycopg.
  2. KHÔNG có I/O -> CI unit-test được. Đúng nguyên tắc của project: CI chỉ chạy
     thứ không cần hạ tầng thật.

⚠️ MỌI CON SỐ DƯỚI ĐÂY LÀ DO NGƯỜI VIẾT CHỌN, KHÔNG ĐO TỪ DỮ LIỆU NGÀNH.
Thư viện `Faker` chỉ sinh DANH TÍNH giả (tên, email, CMND) — nó không biết gì về
hành vi kinh doanh. Hai loại tham số ở đây, bản chất khác hẳn nhau:

  · HÌNH DẠNG (persona, Pareto merchant, mùa vụ, hành vi theo ngành hàng):
    mô phỏng quy luật CÓ THẬT, nhưng ĐỘ LỚN là ước lượng. Production phải tune lại
    từ phân phối thật — cùng tinh thần với ngưỡng 20%/40% trong
    `assert_rowcount_not_dropped.sql`.

  · BỐN SỰ CỐ: KHÔNG cố trông giống thật. Chúng được CẤY CÓ CHỦ ĐÍCH để làm ĐÁP ÁN
    cho bộ test phân tích — ta biết trước merchant nào tắt, ngày nào, sâu bao nhiêu,
    nên đo được hệ thống phân tích có tìm đúng nguyên nhân hay chỉ đoán bừa.
    Không có đáp án biết trước thì không đánh giá được, chỉ có thể tin.
"""
from __future__ import annotations

import decimal
import random
from datetime import date, timedelta

# =============================================================================
# 1. PERSONA — archetype hành vi, không phải "khách to / khách nhỏ"
# =============================================================================
# Một khái niệm chi phối cùng lúc BỐN thứ: tần suất, biên tiền, kênh ưa dùng, có
# nhận lương hay không. Ý tưởng lấy từ `bank-data-simulator`, thay cho cách chia
# nhị phân RETAIL/CORPORATE ban đầu — cách cũ không nói được gì về hành vi.
#
# `activity` là TRỌNG SỐ CHỌN TÀI KHOẢN. Trước đây faker chọn tài khoản bằng
# random đều tuyệt đối, nên "số tài khoản hoạt động" luôn = tổng số tài khoản và
# NHÂN TỬ ĐẦU TIÊN của phép phân rã doanh số chết cứng:
#     Doanh số = Số TK hoạt động × Giao dịch/TK × Giá trị/giao dịch
# DORMANT tồn tại chính là để nhân tử đó biết di chuyển.
PERSONAS: dict[str, dict] = {
    "SALARIED": {
        "share": 0.45, "activity": 1.00, "salaried": True,
        "amount": (20_00, 1_500_00),
        "channels": {"ATM": 0.15, "POS": 0.35, "ONLINE": 0.15, "MOBILE": 0.30, "BRANCH": 0.05},
    },
    "DIGITAL_NATIVE": {
        # Giao dịch dày, giá trị nhỏ, KHÔNG BAO GIỜ tới quầy.
        "share": 0.20, "activity": 2.20, "salaried": True,
        "amount": (10_00, 400_00),
        "channels": {"ATM": 0.04, "POS": 0.16, "ONLINE": 0.35, "MOBILE": 0.45, "BRANCH": 0.00},
    },
    "TRADITIONAL": {
        "share": 0.15, "activity": 0.50, "salaried": False,
        "amount": (50_00, 2_000_00),
        "channels": {"ATM": 0.40, "POS": 0.25, "ONLINE": 0.03, "MOBILE": 0.07, "BRANCH": 0.25},
    },
    "CORPORATE": {
        # Rất ít giao dịch nhưng giá trị lớn -> một mình nhóm này đủ kéo lệch
        # "giá trị trung bình mỗi giao dịch" của cả ngân hàng.
        "share": 0.10, "activity": 0.35, "salaried": False,
        "amount": (2_000_00, 80_000_00),
        "channels": {"ATM": 0.02, "POS": 0.08, "ONLINE": 0.45, "MOBILE": 0.20, "BRANCH": 0.25},
    },
    "DORMANT": {
        # ~1 giao dịch mỗi vài tuần. Nhóm này làm "tài khoản hoạt động" thành số THẬT.
        "share": 0.10, "activity": 0.02, "salaried": False,
        "amount": (20_00, 500_00),
        "channels": {"ATM": 0.35, "POS": 0.30, "ONLINE": 0.10, "MOBILE": 0.15, "BRANCH": 0.10},
    },
}
PERSONA_NAMES = list(PERSONAS)
CHANNELS = ["ATM", "POS", "ONLINE", "MOBILE", "BRANCH"]

# =============================================================================
# 2. ĐỊA LÝ — vùng gộp được, thành phố thì không
# =============================================================================
# `fake.city()` sinh ra hàng trăm thành phố đơn lẻ; gộp nhóm theo nó là vô nghĩa
# (mỗi nhóm một dòng). Bảng cố định dưới đây cho một chiều cắt CÓ THỨ BẬC:
# vùng -> thành phố, đúng thứ mọi dashboard ngân hàng mở ra là thấy.
# Trọng số phản ánh việc hoạt động ngân hàng dồn vào vài đô thị lớn.
REGIONS: dict[str, dict] = {
    "Metro North": {"weight": 0.26,
                    "cities": ["Northgate", "Ashbourne", "Fairhaven", "Kingsport"]},
    "Metro South": {"weight": 0.22,
                    "cities": ["Southbay", "Rivermead", "Clearwater", "Lindon"]},
    "Central": {"weight": 0.18,
                "cities": ["Midvale", "Stonebridge", "Hollowfield", "Elmcrest"]},
    "Coastal East": {"weight": 0.15,
                     "cities": ["Harborview", "Saltmarsh", "Pearl Cove", "Windmere"]},
    "Coastal West": {"weight": 0.12,
                     "cities": ["Westport", "Sunridge", "Baywood", "Marisol"]},
    "Highlands": {"weight": 0.07,
                  "cities": ["Ironpeak", "Aspenvale", "Coldbrook", "Summit Hill"]},
}
REGION_NAMES = list(REGIONS)
REGION_WEIGHTS = [REGIONS[r]["weight"] for r in REGION_NAMES]

# =============================================================================
# 3. NGÀNH HÀNG — mỗi ngành có tần suất và biên tiền RIÊNG
# =============================================================================
# Trước đây ngành gắn ngẫu nhiên cho merchant và không ảnh hưởng gì tới giao dịch,
# nên "phân tích theo ngành" chỉ ra nhiễu. Grocery là mua nhiều lần giá trị nhỏ;
# Travel là hiếm nhưng rất lớn — hai hình dạng hoàn toàn khác nhau.
CATEGORY_PROFILE: dict[str, dict] = {
    "Grocery":   {"freq": 0.28, "amount_mult": 0.40},
    "Dining":    {"freq": 0.20, "amount_mult": 0.50},
    "Retail":    {"freq": 0.18, "amount_mult": 1.00},
    "Fuel":      {"freq": 0.14, "amount_mult": 0.70},
    "Utilities": {"freq": 0.08, "amount_mult": 1.20},
    "Health":    {"freq": 0.07, "amount_mult": 2.00},
    "Travel":    {"freq": 0.05, "amount_mult": 4.00},
}
CATEGORIES = list(CATEGORY_PROFILE)
CATEGORY_FREQ = [CATEGORY_PROFILE[c]["freq"] for c in CATEGORIES]

# alpha=2.0 -> merchant lớn nhất ~8,6%, top 20% ~50%. Đã thử 1.16 ("80/20" kinh
# điển) nhưng cho merchant top tới 33% — một mình nó tắt là sập cả tháng, và khi
# một thực thể áp đảo đến vậy thì phép "tìm điểm tập trung" trở nên tầm thường.
MERCHANT_PARETO_ALPHA = 2.0
# Seed CỐ ĐỊNH riêng cho trọng số merchant: seed và Draw dựng trọng số ở hai thời
# điểm khác nhau, nếu phụ thuộc trạng thái RNG chung thì hai bên ra hai bộ trọng số
# khác nhau và cặp "merchant lớn <-> ngành hàng" gán lúc seed sẽ lệch khỏi thực tế
# lúc rút mẫu. Tách seed là cách rẻ nhất để hai bên luôn nhìn thấy cùng một thế giới.
MERCHANT_WEIGHT_SEED = 20260919

# =============================================================================
# 4. MÙA VỤ VÀ CHU KỲ LƯƠNG
# =============================================================================
WEEKEND_FACTOR = 0.65          # cuối tuần giao dịch ít hơn hẳn
MONTHLY_GROWTH = 0.02          # +2%/tháng — để một tháng SỤT nổi bật lên

# Lương về ngày 1 và 15. KHÔNG còn là hệ số nhân gắn tay như bản trước: giờ mỗi
# tài khoản nhận lương sinh ra một giao dịch CREDIT thật, và mức chi tiêu nhích lên
# trong vài ngày sau đó. Nhịp ngày lương vì vậy NỔI LÊN TỪ DỮ LIỆU chứ không phải
# được áp từ ngoài vào — khác biệt quan trọng khi ai đó hỏi "số này từ đâu ra".
PAYDAYS = (1, 15)
POST_PAYDAY_DAYS = 3           # số ngày chi tiêu còn nhỉnh sau khi lương về
POST_PAYDAY_LIFT = 0.20        # +20% trong những ngày đó
SALARY_RANGE = (3_000_00, 12_000_00)

# =============================================================================
# 5. BỐN SỰ CỐ CẤY SẴN — mỗi cái đẩy một nhân tử khác nhau
# =============================================================================
# Bốn cửa sổ TÁCH RỜI nhau có chủ đích: mỗi kỳ có một câu chuyện chủ đạo khác nhau,
# nên khi so kỳ này với kỳ trước thì nhân tử nổi lên phải khác nhau. Chồng lấn hết
# vào một chỗ thì không kiểm chứng được agent có thật sự phân rã hay chỉ luôn chỉ
# vào cùng một thủ phạm.
INCIDENT_MERCHANT_OUTAGE = (75, 68)    # ngày -75..-68: merchant top tắt 8 ngày
INCIDENT_FRAUD_BURST = (62, 58)        # ngày -62..-58: chiến dịch card testing
INCIDENT_CHANNEL_DEGRADE = (50, 40)    # ngày -50..-40: ONLINE lỗi leo dần rồi hồi
INCIDENT_AMOUNT_DECLINE_FROM = 35      # từ ngày -35: CORPORATE giảm giá trị, KHÔNG hồi

# 4% — tỷ lệ auth thất bại thực tế của thẻ/ví thường rơi vào 2-5%. Trước đây để
# 0.15 (di sản từ `random() < 0.85` của faker cũ): một hệ thống hỏng 15% giao dịch
# là hệ thống đang cháy, và nó còn NHẤN CHÌM sự cố kênh ONLINE mà ta cố ý cấy vào.
CHANNEL_BASE_FAILURE = 0.04
CHANNEL_DEGRADE_PEAK = 0.45
DEGRADED_CHANNEL = "ONLINE"
AMOUNT_DECLINE_RATIO = 0.70            # CORPORATE còn 70% giá trị cũ
DECLINING_PERSONA = "CORPORATE"

# ---- Gian lận ---------------------------------------------------------------
# ⚠️ BÀI HỌC từ `fake-banking-data`: ở tỷ lệ gian lận THẬT (~0,1%) thì mẫu gian lận
# "không nhìn thấy được" — kể cả bằng mắt lẫn bằng máy. Nên gian lận ở đây cấy
# theo CẤU TRÚC (một chuỗi giao dịch nhỏ dồn trong ít phút, phần lớn thất bại),
# KHÔNG phải bằng cách dịch nhẹ phân phối giờ. Cấu trúc thì tìm được và chứng minh
# được; phân phối lệch nhẹ thì không ai bác bỏ nổi kết luận nào.
FRAUD_BASE_RATE = 0.0005               # nền rải rác, để tỷ lệ cảnh báo có ý nghĩa
FRAUD_BURST_ACCOUNTS_PER_DAY = 3       # số tài khoản bị tấn công mỗi ngày trong cửa sổ
FRAUD_BURST_SIZE = (30, 50)            # số giao dịch dò thẻ trong một đợt
FRAUD_BURST_AMOUNT = (1_00, 30_00)     # dò thẻ luôn là số tiền NHỎ
FRAUD_BURST_FAIL_RATE = 0.80           # phần lớn thất bại — đó là bản chất của dò thẻ
# Hệ thống phát hiện KHÔNG hoàn hảo: bỏ sót 30% và báo nhầm 0,1%. Có cả hai thì
# "độ chính xác cảnh báo" mới là câu hỏi có thật để phân tích.
FRAUD_DETECTION_RECALL = 0.70
FRAUD_FALSE_POSITIVE_RATE = 0.001

# ---- issue #002 / #004 ------------------------------------------------------
TRANSFER_SHARE = 0.15      # 15% giao dịch là chuyển khoản liên tài khoản
RETRY_EVERY = 300          # 1/300 lần insert dùng lại khoá cũ -> UNIQUE từ chối


# =============================================================================
# Hàm
# =============================================================================
def build_merchant_weights(n_merchants: int, rng: random.Random | None = None) -> list[float]:
    """Trọng số power-law, chuẩn hoá về tổng = 1, sắp giảm dần.

    Pareto thay vì tuyến tính vì phân bố doanh số theo merchant trong thực tế là
    đuôi dài: vài merchant lớn nuốt phần lớn, phần còn lại rất nhỏ. Index 0 là
    merchant lớn nhất — cũng là nơi cấy sự cố ngừng hoạt động.
    """
    r = rng or random.Random(MERCHANT_WEIGHT_SEED)
    raw = [r.paretovariate(MERCHANT_PARETO_ALPHA) for _ in range(n_merchants)]
    raw.sort(reverse=True)
    total = sum(raw)
    return [w / total for w in raw]


def assign_merchant_categories(weights: list[float]) -> list[str]:
    """Phân ngành cho merchant sao cho DOANH SỐ mỗi ngành xấp xỉ CATEGORY_FREQ.

    KHÔNG rút ngẫu nhiên. Với phân bố Pareto chỉ cần vài merchant lớn vô tình rơi
    cùng một ngành là ngành đó chiếm 40% còn ngành khác còn 1% — đo thật: Fuel
    38,7% / Health 1,2%. Phân tích theo ngành khi đó lệch lạc mà không ai chủ ý.

    Ở đây duyệt merchant theo thứ tự trọng số GIẢM DẦN và luôn giao cho ngành đang
    thiếu nhiều nhất so với mục tiêu — phân bổ theo phần dư lớn nhất, tất định.
    """
    assigned = {c: 0.0 for c in CATEGORIES}
    out, done = [], 0.0
    for w in weights:
        done += w
        c = max(CATEGORIES,
                key=lambda k: CATEGORY_PROFILE[k]["freq"] * done - assigned[k])
        assigned[c] += w
        out.append(c)
    return out


def assign_personas(n_accounts: int, rng: random.Random) -> list[str]:
    """Gán persona cho từng account. Index 0-based, khớp account_id - 1."""
    shares = [PERSONAS[p]["share"] for p in PERSONA_NAMES]
    return rng.choices(PERSONA_NAMES, weights=shares, k=n_accounts)


def account_activity_weights(personas: list[str]) -> list[float]:
    """Trọng số chọn tài khoản. DORMANT gần như không bao giờ được chọn."""
    return [PERSONAS[p]["activity"] for p in personas]


def pick_region(rng: random.Random) -> tuple[str, str]:
    region = rng.choices(REGION_NAMES, weights=REGION_WEIGHTS, k=1)[0]
    return region, rng.choice(REGIONS[region]["cities"])


def pick_category(rng: random.Random) -> str:
    return rng.choices(CATEGORIES, weights=CATEGORY_FREQ, k=1)[0]


def pick_channel(persona: str, rng: random.Random) -> str:
    w = PERSONAS[persona]["channels"]
    return rng.choices(CHANNELS, weights=[w[c] for c in CHANNELS], k=1)[0]


def days_ago(d: date, today: date) -> int:
    """Số ngày tính ngược từ hôm nay. Hôm nay = 0, hôm qua = 1."""
    return (today - d).days


def _in_window(d: date, today: date, window: tuple[int, int]) -> bool:
    start, end = window
    return end <= days_ago(d, today) <= start


def day_volume_factor(d: date, today: date) -> float:
    """Hệ số nhân sản lượng của một ngày: mùa vụ tuần + sau lương + tăng trưởng."""
    f = 1.0
    if d.weekday() >= 5:
        f *= WEEKEND_FACTOR
    # Vài ngày sau khi lương về thì chi tiêu nhỉnh lên — hệ quả của lương, không
    # phải một cú nhảy đúng ngày 1 rồi tắt ngay.
    if any(0 <= (d.day - p) < POST_PAYDAY_DAYS for p in PAYDAYS):
        f *= 1.0 + POST_PAYDAY_LIFT
    months_back = days_ago(d, today) / 30.0
    f *= (1.0 + MONTHLY_GROWTH) ** (-months_back)
    return f


def is_payday(d: date) -> bool:
    return d.day in PAYDAYS


def merchant_is_down(merchant_idx: int, d: date, today: date) -> bool:
    """SỰ CỐ #1 — merchant lớn nhất ngừng hoạt động vài ngày.

    Đẩy nhân tử SỐ GIAO DỊCH, tập trung vào MỘT thực thể -> ca mà phép "tìm điểm
    tập trung" phải chỉ ra đúng thủ phạm.
    """
    return merchant_idx == 0 and _in_window(d, today, INCIDENT_MERCHANT_OUTAGE)


def channel_failure_rate(channel: str, d: date, today: date) -> float:
    """SỰ CỐ #2 — tỷ lệ lỗi kênh ONLINE leo dần rồi hồi phục.

    Đẩy nhân tử TỶ LỆ THÀNH CÔNG: số giao dịch không đổi, nhưng ít cái thành tiền
    thật hơn. Đếm giao dịch thô KHÔNG thấy gì, chỉ nhìn net_amount mới thấy — đúng
    lý do project bắt mọi báo cáo đi qua net_amount.
    """
    if channel != DEGRADED_CHANNEL or not _in_window(d, today, INCIDENT_CHANNEL_DEGRADE):
        return CHANNEL_BASE_FAILURE
    start, end = INCIDENT_CHANNEL_DEGRADE
    progress = (start - days_ago(d, today)) / max(start - end, 1)
    return CHANNEL_BASE_FAILURE + (CHANNEL_DEGRADE_PEAK - CHANNEL_BASE_FAILURE) * progress


def fraud_burst_active(d: date, today: date) -> bool:
    """SỰ CỐ #4 — cửa sổ chiến dịch dò thẻ."""
    return _in_window(d, today, INCIDENT_FRAUD_BURST)


def amount_cents(persona: str, category: str, d: date, today: date,
                 rng: random.Random) -> int:
    """Số tiền (cent): biên theo persona × hệ số theo ngành hàng, có SỰ CỐ #3.

    SỰ CỐ #3 — từ một mốc trở đi, persona CORPORATE giảm 30% giá trị mỗi giao dịch
    và KHÔNG hồi phục. Đẩy nhân tử GIÁ TRỊ MỖI GIAO DỊCH: số lượng y nguyên, tỷ lệ
    thành công y nguyên, chỉ tiền nhỏ đi. Không có phép phân rã thì triệu chứng này
    rất dễ bị quy nhầm cho "ít khách".
    """
    lo, hi = PERSONAS[persona]["amount"]
    value = rng.randint(lo, hi) * CATEGORY_PROFILE[category]["amount_mult"]
    if persona == DECLINING_PERSONA and days_ago(d, today) <= INCIDENT_AMOUNT_DECLINE_FROM:
        value *= AMOUNT_DECLINE_RATIO
    return max(int(value), 1)


def salary_cents(rng: random.Random) -> int:
    return rng.randint(*SALARY_RANGE)


def fraud_detected(is_fraud: bool, rng: random.Random) -> bool:
    """Hệ thống có gắn cờ không — cố ý KHÔNG hoàn hảo (bỏ sót + báo nhầm)."""
    if is_fraud:
        return rng.random() < FRAUD_DETECTION_RECALL
    return rng.random() < FRAUD_FALSE_POSITIVE_RATE


def to_decimal(cents: int) -> decimal.Decimal:
    return decimal.Decimal(cents) / 100


def date_range(today: date, n_days: int, include_today: bool = False):
    """Các ngày của cửa sổ backfill, cũ nhất trước.

    `include_today` (issue #007): bản cũ dừng ở HÔM QUA, còn luồng live bắt đầu vào
    lúc chạy thật — nên ngày giao nhau có một LỖ THẬT từ 00:00 tới giờ khởi động.
    Cổng `assert_rowcount_not_dropped` báo đỏ vì lỗ đó và chặn push_marts suốt tới
    nửa đêm. Cổng báo đúng; thứ phải sửa là cái lỗ.
    """
    for i in range(n_days, 0, -1):
        yield today - timedelta(days=i)
    if include_today:
        yield today
