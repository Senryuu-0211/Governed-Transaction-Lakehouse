"""Unit test cho thế giới mô phỏng của faker.

Trước 09-2026 faker hoàn toàn nằm NGOÀI tầm test: mọi thứ trộn chung với code ghi
DB, nên muốn kiểm một giả định thì phải dựng cả Postgres. Tách `world.py` ra làm
logic thuần chính là để có file này.

Ở đây KHÔNG test "hàm có chạy không" — mà test những GIẢ ĐỊNH THIẾT KẾ mà nếu ai
đó sửa hằng số làm gãy thì dữ liệu vẫn sinh ra bình thường, không lỗi, chỉ là vô
dụng cho phân tích. Đó là kiểu hỏng im lặng mà project này sợ nhất.
"""

import random
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "faker"))

import world  # noqa: E402

TODAY = date(2026, 9, 19)


def rng():
    return random.Random(42)


# ---- Persona -----------------------------------------------------------------
def test_persona_shares_sum_to_one():
    assert sum(p["share"] for p in world.PERSONAS.values()) == pytest.approx(1.0)


def test_dormant_accounts_are_actually_dormant():
    """Nhân tử "SỐ TÀI KHOẢN HOẠT ĐỘNG" chỉ tồn tại nhờ nhóm này.

    Bản trước chọn tài khoản bằng random ĐỀU tuyệt đối, nên mọi tài khoản đều giao
    dịch mỗi ngày -> số tài khoản hoạt động luôn = tổng số tài khoản, và nhân tử
    ĐẦU TIÊN của phép phân rã doanh số chết cứng. Không có gì báo lỗi cả.
    """
    personas = world.assign_personas(20_000, rng())
    w = world.account_activity_weights(personas)
    total = sum(w)
    share = defaultdict(float)
    for p, x in zip(personas, w, strict=True):
        share[p] += x / total

    assert share["DORMANT"] < 0.01, "DORMANT phải gần như không giao dịch"
    assert share["DIGITAL_NATIVE"] > share["DORMANT"] * 50


def test_personas_differ_in_amount_and_channel():
    r = rng()

    def avg(persona):
        return sum(world.amount_cents(persona, "Retail", TODAY, TODAY, r)
                   for _ in range(2000)) / 2000

    assert avg("CORPORATE") > avg("SALARIED") * 5
    assert avg("DIGITAL_NATIVE") < avg("SALARIED")
    # Khách số hoá không bao giờ tới quầy — nếu mọi persona dùng kênh như nhau thì
    # "phân tích theo kênh" chỉ ra nhiễu.
    assert world.PERSONAS["DIGITAL_NATIVE"]["channels"]["BRANCH"] == 0.0
    assert world.PERSONAS["TRADITIONAL"]["channels"]["BRANCH"] > 0.1


# ---- Merchant & ngành hàng ---------------------------------------------------
def test_merchant_concentration_in_useful_band():
    """Không quá đều mà cũng không để một merchant áp đảo.

    Đã dính thật: alpha=1.16 ("80/20" kinh điển) cho merchant top tới 33% — một
    mình nó tắt là sập cả tháng, và phép "tìm điểm tập trung" thành tầm thường vì
    câu trả lời lúc nào cũng là nó.
    """
    w = world.build_merchant_weights(200)
    assert 0.03 < w[0] < 0.15, f"merchant top chiếm {w[0]:.1%} — ngoài biên dùng được"
    assert 0.40 < sum(w[:40]) < 0.65
    assert w == sorted(w, reverse=True)
    assert sum(w) == pytest.approx(1.0)


def test_merchant_weights_stable_without_rng():
    """Seed và Draw dựng trọng số ở hai thời điểm khác nhau — phải ra CÙNG kết quả.

    Nếu lệch thì cặp "merchant lớn <-> ngành hàng" gán lúc seed không còn khớp với
    thực tế lúc rút mẫu, và toàn bộ phân bổ ngành hàng trôi đi trong im lặng.
    """
    assert world.build_merchant_weights(50) == world.build_merchant_weights(50)


def test_category_allocation_matches_target_share():
    """Đã dính thật: rút ngẫu nhiên cho Fuel 38,7% còn Health 1,2%.

    Vài merchant lớn vô tình rơi cùng một ngành là đủ làm lệch, mà không ai chủ ý
    và không có gì báo lỗi.
    """
    w = world.build_merchant_weights(200)
    cats = world.assign_merchant_categories(w)
    got = defaultdict(float)
    for c, x in zip(cats, w, strict=True):
        got[c] += x
    for c in world.CATEGORIES:
        assert got[c] == pytest.approx(world.CATEGORY_PROFILE[c]["freq"], abs=0.02)


def test_category_changes_transaction_size():
    r = rng()

    def avg(cat):
        return sum(world.amount_cents("SALARIED", cat, TODAY, TODAY, r)
                   for _ in range(2000)) / 2000

    assert avg("Travel") > avg("Grocery") * 5, "Travel hiếm nhưng phải LỚN"


# ---- Mùa vụ và lương ---------------------------------------------------------
def test_weekend_is_quieter_than_weekday():
    sat = world.day_volume_factor(date(2026, 9, 12), TODAY)
    fri = world.day_volume_factor(date(2026, 9, 11), TODAY)
    assert sat < fri * 0.8


def test_spending_lifts_in_days_after_payday():
    """Nhịp ngày lương phải NỔI LÊN từ giao dịch lương thật + chi tiêu sau đó,
    không phải một cú nhảy đúng ngày 1 rồi tắt ngay."""
    assert world.is_payday(date(2026, 9, 15))
    assert not world.is_payday(date(2026, 9, 20))
    after = world.day_volume_factor(date(2026, 9, 16), TODAY)
    quiet = world.day_volume_factor(date(2026, 9, 20), TODAY)
    assert after > quiet


def test_growth_makes_older_days_smaller():
    old = world.day_volume_factor(TODAY - timedelta(days=84), TODAY)
    new = world.day_volume_factor(TODAY - timedelta(days=7), TODAY)
    assert old < new


# ---- Bốn sự cố ---------------------------------------------------------------
def test_outage_hits_only_top_merchant_and_only_in_window():
    start, end = world.INCIDENT_MERCHANT_OUTAGE
    inside = TODAY - timedelta(days=(start + end) // 2)
    outside = TODAY - timedelta(days=start + 10)
    assert world.merchant_is_down(0, inside, TODAY)
    assert not world.merchant_is_down(0, outside, TODAY)
    assert not world.merchant_is_down(1, inside, TODAY), "chỉ merchant top được tắt"


def test_channel_degradation_ramps_then_recovers():
    start, end = world.INCIDENT_CHANNEL_DEGRADE
    before = world.channel_failure_rate("ONLINE", TODAY - timedelta(days=start + 5), TODAY)
    mid = world.channel_failure_rate("ONLINE", TODAY - timedelta(days=(start + end) // 2), TODAY)
    peak = world.channel_failure_rate("ONLINE", TODAY - timedelta(days=end), TODAY)
    after = world.channel_failure_rate("ONLINE", TODAY - timedelta(days=end - 5), TODAY)
    assert before == world.CHANNEL_BASE_FAILURE
    assert before < mid < peak
    assert peak == pytest.approx(world.CHANNEL_DEGRADE_PEAK)
    assert after == world.CHANNEL_BASE_FAILURE, "phải HỒI PHỤC, không kéo dài mãi"


def test_degradation_does_not_leak_to_other_channels():
    mid = TODAY - timedelta(days=45)
    for ch in world.CHANNELS:
        if ch != world.DEGRADED_CHANNEL:
            assert world.channel_failure_rate(ch, mid, TODAY) == world.CHANNEL_BASE_FAILURE


def test_base_failure_rate_is_plausible_for_a_bank():
    """15% giao dịch hỏng là một hệ thống đang cháy, không phải nền bình thường.

    Con số cũ (0.15) là di sản từ `random() < 0.85` của faker đời đầu, và nó còn
    NHẤN CHÌM sự cố kênh mà ta cố ý cấy vào.
    """
    assert 0.01 <= world.CHANNEL_BASE_FAILURE <= 0.08
    assert world.CHANNEL_DEGRADE_PEAK > world.CHANNEL_BASE_FAILURE * 5


def test_amount_decline_hits_one_persona_only_and_does_not_recover():
    before = TODAY - timedelta(days=world.INCIDENT_AMOUNT_DECLINE_FROM + 10)
    after = TODAY - timedelta(days=world.INCIDENT_AMOUNT_DECLINE_FROM - 10)

    def avg(persona, day):
        r = random.Random(7)
        return sum(world.amount_cents(persona, "Retail", day, TODAY, r)
                   for _ in range(3000)) / 3000

    hit = world.DECLINING_PERSONA
    assert avg(hit, after) < avg(hit, before) * 0.85
    assert avg("SALARIED", after) == pytest.approx(avg("SALARIED", before), rel=0.1)
    assert avg(hit, TODAY) < avg(hit, before) * 0.85, "sự cố này KHÔNG hồi phục"


def test_fraud_burst_only_inside_its_window():
    start, end = world.INCIDENT_FRAUD_BURST
    assert world.fraud_burst_active(TODAY - timedelta(days=(start + end) // 2), TODAY)
    assert not world.fraud_burst_active(TODAY - timedelta(days=start + 5), TODAY)
    assert not world.fraud_burst_active(TODAY, TODAY)


def test_fraud_detection_is_deliberately_imperfect():
    """Có bỏ sót VÀ báo nhầm thì "độ chính xác cảnh báo" mới là câu hỏi có thật.

    Nếu cờ nghi ngờ luôn trùng khít sự thật thì mọi phân tích về hiệu quả phát
    hiện gian lận đều cho 100% — đúng nhưng vô nghĩa.
    """
    r = rng()
    caught = sum(world.fraud_detected(True, r) for _ in range(5000)) / 5000
    false_alarm = sum(world.fraud_detected(False, r) for _ in range(20000)) / 20000
    assert 0.5 < caught < 0.95, "phải BỎ SÓT một phần"
    assert 0 < false_alarm < 0.01, "phải có BÁO NHẦM"


def test_incident_windows_do_not_overlap():
    """Bất biến THIẾT KẾ, không phải chi tiết code.

    Bốn sự cố là ĐÁP ÁN của bộ test phân tích: mỗi kỳ phải có đúng một câu chuyện
    chủ đạo. Chồng lấn thì không còn phân biệt được agent thật sự phân rã nhân tử
    hay chỉ luôn chỉ vào cùng một thủ phạm — và không ai nhận ra, vì dữ liệu vẫn
    sinh ra bình thường.
    """
    def span(w):
        return set(range(w[1], w[0] + 1))

    windows = {
        "outage": span(world.INCIDENT_MERCHANT_OUTAGE),
        "fraud": span(world.INCIDENT_FRAUD_BURST),
        "degrade": span(world.INCIDENT_CHANNEL_DEGRADE),
        "decline": set(range(0, world.INCIDENT_AMOUNT_DECLINE_FROM + 1)),
    }
    names = list(windows)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            assert not windows[a] & windows[b], f"{a} chồng lấn {b}"


# ---- Cấu hình ----------------------------------------------------------------
def test_live_rate_matches_backfill_density():
    """`.env.example` phải giữ nhịp live khớp mật độ lịch sử.

    Đã dính thật: TXN_RATE=20/giây = 1,7 TRIỆU giao dịch/ngày trong khi backfill
    chỉ 20.000/ngày -> 17 phút chạy live là bằng cả một ngày lịch sử, "hôm nay"
    vĩnh viễn là ngoại lệ khổng lồ, và mọi phép so sánh kỳ trở nên vô nghĩa.
    Không có gì báo lỗi — dữ liệu vẫn sinh ra đều đặn.
    """
    env = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        line = line.split("#")[0].strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()

    per_day_live = float(env["TXN_RATE"]) * 86_400
    per_day_hist = int(env["BACKFILL_TXN_PER_DAY"])
    assert 0.5 < per_day_live / per_day_hist < 2.0

    # Và mật độ đó phải ra số giao dịch/tài khoản/ngày mà con người thật sự tiêu.
    per_account = per_day_hist / int(env["N_ACCOUNTS"])
    assert 1 < per_account < 10, f"{per_account:.1f} giao dịch/tài khoản/ngày là phi thực tế"


def test_deterministic_with_same_seed():
    """Cùng seed phải ra cùng data — điều kiện để reconciliation test tái lập được."""
    assert world.assign_personas(100, random.Random(1)) == \
           world.assign_personas(100, random.Random(1))
    assert world.pick_region(random.Random(1)) == world.pick_region(random.Random(1))


def test_backfill_covers_today_so_there_is_no_gap_at_the_handover():
    """issue #007 — lỗ ở ngày giao nhau giữa lịch sử và luồng live.

    Bản cũ `date_range` dừng ở HÔM QUA, còn luồng live bắt đầu lúc khởi động. Ngày
    giao nhau vì vậy thiếu hẳn phần từ 00:00 tới giờ chạy — đo được 36k giao dịch so
    với ~100k của mọi ngày khác. Cổng `assert_rowcount_not_dropped` báo đỏ và chặn
    push_marts cho tới nửa đêm, mà báo ĐÚNG: lỗ là có thật.

    Test này gác cái hợp đồng đó, không gác cách cài đặt.
    """
    today = date(2026, 9, 23)
    without = list(world.date_range(today, 5))
    with_today = list(world.date_range(today, 5, include_today=True))

    assert today not in without, "mặc định phải giữ nguyên hành vi cũ"
    assert with_today[-1] == today, "hôm nay phải là ngày CUỐI, không phải chen giữa"
    assert with_today[:-1] == without, "các ngày lịch sử không được đổi"
    # Không có ngày nào bị trùng hay nhảy cóc: chuỗi phải liền mạch.
    gaps = {(b - a).days for a, b in zip(with_today[:-1], with_today[1:], strict=True)}
    assert gaps == {1}, f"chuỗi ngày phải liền mạch, thấy khoảng cách {gaps}"
