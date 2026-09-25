"""Test thư viện phân tích — phần thuần logic, CI chạy được không cần DB.

Mỗi test gác một cách nói dối cụ thể. Cái quan trọng nhất
(`test_a_group_that_vanished_ranks_first`) gác một lỗi ĐÃ XẢY RA THẬT: tool bỏ
sót đúng loại sự cố nó sinh ra để tìm.
"""
import math
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "semantic"))

from tools import (  # noqa: E402
    _log_mean,
    _shift_month,
    diff_rows,
    herfindahl,
    period_warnings,
    robust_z,
)


def test_lmdi_leaves_no_residual_whatever_the_numbers():
    """Lý do duy nhất chọn LMDI thay vì phép chia ngây thơ: số dư bằng 0 theo
    ĐỊNH NGHĨA. Nếu một ngày nào đó test này đỏ thì công thức đã bị sửa hỏng, và
    con số 'đã giải thích hết' trở thành lời nói dối."""
    cases = [
        ((100.0, 5.0, 20.0), (80.0, 6.0, 15.0)),    # cả ba cùng đổi
        ((50.0, 2.0, 10.0), (200.0, 1.0, 40.0)),    # đổi ngược chiều nhau
        ((10.0, 10.0, 10.0), (10.0, 10.0, 11.0)),   # chỉ một cái đổi
        ((3.0, 7.0, 11.0), (3.0, 7.0, 11.0)),       # không đổi gì
    ]
    for base, comp in cases:
        v0 = base[0] * base[1] * base[2]
        v1 = comp[0] * comp[1] * comp[2]
        lm = _log_mean(v1, v0)
        contrib = sum(lm * math.log(c / b) for b, c in zip(base, comp, strict=True))
        assert math.isclose(contrib, v1 - v0, abs_tol=1e-9), (
            f"số dư {v1 - v0 - contrib} với {base} -> {comp}")


def test_log_mean_refuses_zero_instead_of_inventing_a_number():
    """Ang đề xuất thay 0 bằng một δ rất nhỏ. Làm thế là BỊA ra một con số rồi
    trình bày như kết quả — thà nói thẳng 'không phân rã được'."""
    assert _log_mean(0.0, 5.0) is None
    assert _log_mean(5.0, 0.0) is None
    assert _log_mean(-1.0, 5.0) is None
    assert _log_mean(7.0, 7.0) == 7.0          # L(a,a) = a


def test_a_group_that_vanished_ranks_first():
    """LỖI ĐÃ XẢY RA THẬT: merchant tắt hẳn không có dòng ở kỳ sau -> delta=None
    -> xếp theo abs(None or 0)=0 -> rơi xuống CUỐI bảng. Tool tìm sự cố mà bỏ
    sót đúng loại sự cố rõ nhất, và chỉ lộ ra nhờ có đáp án để đối chiếu."""
    base = {("Tắt hẳn",): 54_000_000.0, ("Nhỏ",): 100.0, ("Bình thường",): 20_000_000.0}
    comp = {("Nhỏ",): 400.0, ("Bình thường",): 23_000_000.0}
    rows = diff_rows(base, comp)
    assert rows[0]["key"] == ["Tắt hẳn"]
    assert rows[0]["disappeared"] is True
    assert rows[0]["comp"] == 0.0
    assert math.isclose(rows[0]["pct_change"], -1.0)


def test_a_brand_new_group_gets_no_fabricated_percentage():
    """Nhóm mới không có mẫu số. '+∞%' hay '+100%' đều là con số bịa."""
    rows = diff_rows({}, {("Mới",): 500.0})
    assert rows[0]["appeared"] is True
    assert rows[0]["base"] == 0.0
    assert rows[0]["pct_change"] is None


def test_ranking_uses_absolute_change_not_percentage():
    """Xếp theo phần trăm thì một nhóm bé tí nhảy từ 2 lên 6 (+200%) luôn đứng
    đầu và che mất thứ thật sự đáng nhìn."""
    rows = diff_rows({("bé",): 2.0, ("to",): 1_000_000.0},
                     {("bé",): 6.0, ("to",): 1_200_000.0})
    assert rows[0]["key"] == ["to"]


def test_period_warnings_catch_the_two_classic_lies():
    """Hai kỳ dài khác nhau, và hai kỳ lệch cơ cấu thứ — cuối tuần chỉ bằng ~65%
    ngày thường nên một kỳ nhiều cuối tuần hơn sẽ 'giảm' mà chẳng có gì xảy ra."""
    w = period_warnings("2026-07-03", "2026-07-09", "2026-07-10", "2026-07-14")
    assert any("dài khác nhau" in x for x in w)
    assert any("cuối tuần" in x for x in w)

    # Hai kỳ 7 ngày liền nhau: cùng độ dài, cùng cơ cấu thứ. KHÔNG khẳng định là
    # im lặng hoàn toàn — cảnh báo chu kỳ THÁNG vẫn bắn, và bắn đúng: hai tuần
    # liền nhau rơi vào hai nửa khác nhau của tháng.
    w2 = period_warnings("2026-07-03", "2026-07-09", "2026-07-10", "2026-07-16")
    assert not any("dài khác nhau" in x for x in w2)
    assert not any("cuối tuần" in x for x in w2)

    # Cùng chỗ đứng trong tháng, cùng cơ cấu thứ, cùng độ dài -> im lặng thật.
    assert period_warnings("2026-06-04", "2026-06-10", "2026-07-02", "2026-07-08") == []


def test_a_period_containing_today_is_flagged():
    """Ngày chưa đóng sổ luôn trông như tụt — đúng cái đã chặn push_marts hôm 23-09."""
    today = date.today()
    w = period_warnings((today - timedelta(days=3)).isoformat(), today.isoformat(),
                        (today - timedelta(days=7)).isoformat(),
                        (today - timedelta(days=4)).isoformat())
    assert any("chưa đóng sổ" in x for x in w)


# --- tập trung & quy trách ---------------------------------------------------


def test_hhi_matches_the_two_reference_points():
    """Thang DOJ/FTC neo ở hai mốc: độc quyền = 10.000, N đối thủ đều nhau =
    10.000/N. Sai thang thì mọi ngưỡng 1.500/2.500 trở nên vô nghĩa."""
    assert math.isclose(herfindahl([100.0]), 10_000)
    assert math.isclose(herfindahl([25.0] * 4), 2_500)
    assert math.isclose(herfindahl([1.0] * 50), 200)
    # Thang bất biến theo đơn vị: đổi tiền tệ không được đổi mức tập trung.
    assert math.isclose(herfindahl([3.0, 1.0]), herfindahl([3_000.0, 1_000.0]))


def test_effective_entities_is_the_readable_form_of_hhi():
    """10.000/HHI = 'tương đương bao nhiêu đối tác đều nhau' — câu nói được với
    người không rành kỹ thuật, khác hẳn 'HHI = 148'."""
    assert math.isclose(10_000 / herfindahl([10.0] * 8), 8.0)


def test_hhi_of_nothing_does_not_explode():
    assert herfindahl([]) == 0.0
    assert herfindahl([0.0, 0.0]) == 0.0


def test_offsetting_moves_are_visible_in_gross_not_net():
    """Doanh số có thể ĐỨNG YÊN trong khi nửa thị trường sập và nửa kia tăng gấp
    đôi. Chỉ nhìn thay đổi thuần là bỏ lỡ đúng điều đáng báo cáo nhất."""
    rows = diff_rows({("A",): 100.0, ("B",): 100.0},
                     {("A",): 0.0, ("B",): 200.0})
    net = sum(r["delta"] for r in rows)
    gross = sum(abs(r["delta"]) for r in rows)
    assert net == 0.0
    assert gross == 200.0
    # share_of_change chia cho 0 -> vô nghĩa; share_of_gross vẫn đọc được.
    assert all(abs(r["delta"]) / gross == 0.5 for r in rows)


def test_periods_in_different_parts_of_the_month_are_flagged():
    """Chu kỳ lương/hoá đơn bám ngày trong tháng. Hai kỳ phủ nửa đầu và nửa sau
    sẽ lệch mà chẳng có gì xảy ra — đúng cái đã làm '+134,9M' trông như một phát
    hiện trong khi phần lớn là ngày lương 15/07."""
    w = period_warnings("2026-07-03", "2026-07-09", "2026-07-10", "2026-07-16")
    assert any("phần khác nhau của tháng" in x for x in w)
    # Cùng chỗ đứng trong tháng ở hai tháng khác nhau -> KHÔNG cảnh báo.
    assert not any("phần khác nhau của tháng" in x
                   for x in period_warnings("2026-06-03", "2026-06-09",
                                            "2026-07-03", "2026-07-09"))


# --- phân kỳ (difference-in-differences) -------------------------------------


def test_robust_z_is_not_dragged_down_by_the_outlier_it_is_looking_for():
    """Lý do dùng median + MAD thay vì trung bình + độ lệch chuẩn: chính cái bất
    thường ta đang tìm sẽ kéo cả hai thống kê kia về phía nó, rồi tự làm mình
    trông bình thường. Đây là bài kiểm tra trực tiếp điều đó."""
    vals = [1.0, 1.1, 0.9, 1.0, 1.05, -25.0]        # một kẻ lệch rất xa
    zs = robust_z(vals)
    assert abs(zs[-1]) > 3.5, "kẻ lệch không bị bắt"
    assert all(abs(z) < 3.5 for z in zs[:-1]), "nhóm bình thường bị bắt oan"

    # Trung bình + độ lệch chuẩn thì KHÔNG bắt được: kẻ lệch tự thổi mẫu số lên.
    mean = sum(vals) / len(vals)
    sd = (sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5
    assert abs((vals[-1] - mean) / sd) < 3.5


def test_robust_z_refuses_to_score_when_there_is_no_scale():
    """MAD = 0 (quá nửa số điểm bằng nhau) thì không có thang để đo lệch. Bịa ra
    một điểm số là tệ hơn im lặng."""
    assert robust_z([5.0, 5.0, 5.0, 5.0]) == [None] * 4
    assert robust_z([1.0, 2.0]) == [None, None]      # quá ít nhóm


def test_control_group_of_a_ratio_is_pooled_not_averaged():
    """Tỷ lệ của phần còn lại = (tổng tử)/(tổng mẫu), KHÔNG phải trung bình các
    tỷ lệ thành viên. Trung bình cho một kênh 10 giao dịch cùng trọng số với một
    kênh 10 triệu giao dịch — sai nghiêm trọng khi quy mô lệch nhau."""
    others = [{"num": 9.0, "den": 10.0}, {"num": 100.0, "den": 1_000_000.0}]
    pooled = sum(o["num"] for o in others) / sum(o["den"] for o in others)
    averaged = sum(o["num"] / o["den"] for o in others) / len(others)
    assert math.isclose(pooled, 109.0 / 1_000_010.0)
    assert averaged > 0.45          # trung bình bị kênh tí hon kéo lên
    assert pooled < 0.001           # gộp đúng thì gần như bằng kênh lớn


def test_placebo_window_keeps_the_position_in_the_month():
    """Kiểm định giả dược phải khớp VỊ TRÍ TRONG THÁNG (kỳ lương, kỳ hoá đơn),
    nên dịch theo tháng lịch chứ không theo 28 hay 30 ngày."""
    assert _shift_month("2026-09-08") == "2026-08-08"
    assert _shift_month("2026-01-15") == "2025-12-15"
    # 31/03 lùi về tháng 2 -> kẹp về ngày cuối tháng, không tràn sang 03/03.
    assert _shift_month("2026-03-31") == "2026-02-28"
    assert _shift_month("2026-05-31") == "2026-04-30"


def test_additive_only_tools_refuse_a_ratio_metric():
    """Tỷ lệ thành công của 5 kênh cộng lại không phải tỷ lệ của cả hệ thống, và
    'BRANCH chiếm 22% tỷ lệ thành công' là câu vô nghĩa. Cả hai phép vẫn ra một con
    số trông hợp lý — nên phải chặn ở cửa, không thể trông vào người đọc."""
    from layer import SemanticLayer
    from tools import explain_change, find_concentration
    layer = SemanticLayer.load()
    r = explain_change(layer, "success_rate", "channel",
                       "2026-08-01", "2026-08-07", "2026-08-08", "2026-08-14")
    assert "error" in r and "detect_divergence" in r["hint"]
    r = find_concentration(layer, "fraud_recall", "channel")
    assert "error" in r


# --- khoảng im lặng -----------------------------------------------------------
def test_zero_runs_finds_only_long_enough_silences():
    from tools import zero_runs
    vals = [5, 0, 5, 0, 0, 0, 5, 0, 0]
    assert zero_runs(vals, 2) == [(3, 5), (7, 8)]      # đoạn 1 ngày bị bỏ
    assert zero_runs(vals, 4) == []
    assert zero_runs([0, 0, 0], 2) == [(0, 2)]


def test_a_silence_that_straddles_any_period_split_is_still_found():
    """LỖI ĐÃ XẢY RA THẬT (eval 25-09): so 1–15/07 với 16–31/07 thì merchant tắt
    10–17/07 bị báo là TĂNG +33 triệu, vì khoảng tắt vắt ngang điểm chia. Ở đây
    không có điểm chia nào để vắt ngang: chuỗi ngày được nhìn liền một mạch."""
    from tools import dropouts_from_series
    days = [date(2026, 7, d) for d in range(1, 32)]
    dead = {d: 100.0 for d in days if not (10 <= d.day <= 17)}
    alive = {d: 100.0 for d in days}
    found, _ = dropouts_from_series({"Tắt": dead, "Sống": alive}, days)
    assert len(found) == 1
    assert found[0]["key"] == "Tắt"
    assert (found[0]["start"], found[0]["end"]) == ("2026-07-10", "2026-07-17")
    assert found[0]["days"] == 8


def test_not_existing_yet_is_not_a_silence():
    """Số 0 trước ngày hoạt động đầu tiên là 'chưa mở cửa', không phải 'tắt'."""
    from tools import dropouts_from_series
    days = [date(2026, 7, d) for d in range(1, 11)]
    newcomer = {d: 50.0 for d in days if d.day >= 6}
    found, _ = dropouts_from_series({"Mới": newcomer}, days)
    assert found == []


def test_naturally_sparse_entities_are_skipped_and_counted():
    """Thực thể ngày thường cũng hay im lặng thì vài ngày im lặng không nói lên gì.
    Bỏ qua — nhưng ĐẾM, không im lặng bỏ."""
    from tools import dropouts_from_series
    days = [date(2026, 7, d) for d in range(1, 21)]
    flaky = {d: 10.0 for d in days if d.day % 3 == 0}          # im lặng 2/3 số ngày
    found, skipped = dropouts_from_series({"Thưa": flaky}, days)
    assert found == [] and skipped == 1


def test_bigger_losses_rank_first():
    """Merchant tí hon im lặng 3 ngày không quan trọng bằng merchant lớn im lặng 2 ngày."""
    from tools import dropouts_from_series
    days = [date(2026, 7, d) for d in range(1, 21)]
    tiny = {d: 1.0 for d in days if d.day not in (5, 6, 7)}
    huge = {d: 10_000.0 for d in days if d.day not in (12, 13)}
    found, _ = dropouts_from_series({"Tí hon": tiny, "Khổng lồ": huge}, days)
    assert [f["key"] for f in found] == ["Khổng lồ", "Tí hon"]
