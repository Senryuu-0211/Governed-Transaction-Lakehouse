"""Test bộ đối chiếu số — tầng gác cuối trước khi câu trả lời tới người dùng.

Tầng semantic bảo đảm số LẤY RA đúng. Tầng này bảo đảm số ĐƯA VÀO CÂU TRẢ LỜI
đúng là số đã lấy ra. Hai chuyện khác nhau, và chỉ cái thứ hai là thứ người dùng
thật sự đọc.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "agent"))

from verify import verify_answer  # noqa: E402

FACTS = [{"rows": [["ONLINE", 0.953], ["ONLINE", 0.700]], "net_change": -54195256.0}]


def test_a_fabricated_ratio_is_caught():
    """LỖI ĐÃ XẢY RA THẬT: ABS_TOL = 0.5 khiến MỌI tỷ lệ bịa đều 'khớp', vì sai
    số cho phép rộng hơn cả miền giá trị 0..1. Bộ gác báo 'sạch' trong khi không
    gác gì cả — tệ hơn là không có bộ gác, vì nó tạo cảm giác an toàn."""
    r = verify_answer("Tỷ lệ thành công là 0,42.", FACTS)
    assert r["unverified"] == ["0,42"]
    assert r["clean"] is False


def test_real_numbers_pass_in_both_number_conventions():
    """Mô hình viết '95,3%' cho 0,953 và '54.195.256' cho -54195256. Cả hai đều
    là cùng một sự thật, chỉ khác cách trình bày và dấu."""
    r = verify_answer("ONLINE 95,3% xuống 70,0%; mất 54.195.256.", FACTS)
    assert r["clean"] is True
    assert r["verified"] == 3


def test_a_number_the_model_computed_itself_is_marked_derived_not_fake():
    """Mô hình được phép tự trừ: 95,3 − 70,0 = 25,3. Đánh nó là bịa thì cảnh báo
    bắn oan liên tục, và cảnh báo bắn oan là cảnh báo không ai đọc."""
    r = verify_answer("Giảm 25,3 điểm phần trăm.", FACTS)
    assert [c["status"] for c in r["numbers"]] == ["derived"]
    assert r["clean"] is True


def test_dates_are_not_mistaken_for_figures():
    """LỖI ĐÃ XẢY RA THẬT: '08–14/09' sinh ra ba 'con số' không truy về đâu được,
    làm cảnh báo bịa-số nhiễu tới mức vô dụng."""
    r = verify_answer("Tuần 08–14/09 so với 01-07/09, ngày 2026-09-14.", FACTS)
    assert r["unverified"] == []
    assert r["clean"] is True


def test_sql_text_is_not_mined_for_numbers():
    """SQL chứa LIMIT 1000, ngày tháng, hằng số — đó là số của CÂU LỆNH, không
    phải số liệu. Gộp chúng vào kho đối chiếu là tự tạo chỗ trú cho số bịa."""
    facts = [{"sql": "select ... limit 4242", "params": [777888], "value": 5.0}]
    r = verify_answer("Kết quả là 4242.", facts)
    assert r["unverified"] == ["4242"]


def test_list_numbering_is_not_a_figure():
    """LỖI ĐÃ XẢY RA THẬT (eval 25-09): câu trả lời ĐÚNG về sự cố CORPORATE bị chấm
    trượt vì '2.', '3.' và 'cảnh báo (3)' bị coi là số bịa."""
    text = "Cảnh báo:\n1. Hai kỳ dài khác nhau.\n2. Lệch cuối tuần.\nXem cảnh báo (2)."
    assert verify_answer(text, FACTS)["unverified"] == []


def test_numbers_quoted_from_a_tool_warning_are_traceable():
    """Cảnh báo viết bằng chữ vẫn là đầu ra của tool. Trích lại nó là trích nguồn."""
    facts = [{"warnings": ["Hai kỳ dài khác nhau (15 ngày so với 16 ngày)"]}]
    r = verify_answer("Hai kỳ dài 15 và 16 ngày.", facts)
    assert r["clean"] is True


def test_internal_fact_ids_do_not_launder_a_fabricated_number():
    """fact_id 'F12' là mã nội bộ. Để nó vào kho thì số 12 bịa sẽ 'khớp'."""
    r = verify_answer("Có 12 merchant tắt.", [{"fact_id": "F12", "value": 999.0}])
    assert r["unverified"] == ["12"]


def test_day_of_month_ranges_are_dates_not_claims():
    """'Nửa đầu T7 (1–15)' và 'đến ngày 31' là vị trí trong tháng, không phải số liệu."""
    r = verify_answer("Nửa đầu T7 (1–15), còn đến ngày 31 thì tăng.", FACTS)
    assert r["unverified"] == []


def test_tolerance_follows_the_precision_the_model_wrote():
    """Một ngưỡng cố định không thoả được cả hai loại số (eval 25-09): tỷ lệ cần
    CHẶT, số nguyên làm tròn cần RỘNG. Viết '8' là tự nhận ±0,5; viết '0,42' là tự
    nhận ±0,005 — chấm theo đúng lời tự nhận."""
    facts = [{"own_change": -7.6, "rate": 0.953}]
    assert verify_answer("Nhóm TRADITIONAL giảm 8 USD.", facts)["clean"] is True
    assert verify_answer("Tỷ lệ 0,95.", facts)["clean"] is True        # ±0,005 quanh 0,953
    # ...nhưng khai chính xác tới 3 chữ số thì phải khớp tới 3 chữ số.
    assert verify_answer("Tỷ lệ 0,948.", facts)["unverified"] == ["0,948"]


def test_ordinals_and_weekdays_are_not_figures():
    """'cảnh báo thứ 3', 'Thứ 2' (thứ trong tuần) — không phải số liệu."""
    assert verify_answer("Theo cảnh báo thứ 3, và vào Thứ 2.", FACTS)["unverified"] == []
