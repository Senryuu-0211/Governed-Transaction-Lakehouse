"""Data faker cho banking source DB — Phase 1 Step 1a.

Mục tiêu: tạo dòng thay đổi THẬT trên Postgres để CDC (Step 1b) có cái mà bắt.
Cố tình trộn 4 loại thao tác để bao phủ mọi op của Debezium về sau:
  - INSERT  : giao dịch mới ở trạng thái PENDING            -> CDC op=c
  - UPDATE  : PENDING -> COMPLETED/FAILED (vòng đời thật)    -> CDC op=u (before/after status)
  - REVERSAL: COMPLETED -> REVERSED (đảo chiều)              -> CDC op=u (nền test REVERSAL Gold)
  - PURGE   : xoá giao dịch FAILED cũ theo chính sách lưu trữ -> CDC op=d (+ tombstone)

KHÔNG mô phỏng một thế giới hoàn hảo: một tỷ lệ nhỏ giao dịch bị TREO ở PENDING
(mô phỏng downstream timeout, không biết tiền đã đi hay chưa). Một job quét sẽ cứu
phần lớn số đó, và cố ý CHỪA LẠI vài dòng treo vĩnh viễn cần can thiệp thủ công.
Lý do: nếu dữ liệu không bao giờ chứa ca xấu thì logic xử lý ca xấu ở Silver/Gold/
alerting KHÔNG BAO GIỜ được chạy — sai mà không ai phát hiện. Bảng nguồn phải chứa
sẵn cả ba nhóm để downstream buộc phải xử lý chúng ngay từ khi viết.

Chỉ dùng dữ liệu SYNTHETIC (Faker) — KHÔNG bao giờ PII thật.
Tốc độ chỉnh qua env TXN_RATE (số INSERT/giây, mặc định 20).

Thiết kế để scale: logic giống nhau ở mọi quy mô; DEMO chạy nhỏ (~100 accounts,
~50 merchants, ~20 txn/s) trên home server — KHÔNG giả vờ chạy tỷ bản ghi.
"""
import decimal
import os
import random
import time

import psycopg

from faker import Faker

fake = Faker()

# Seed để data TÁI LẬP được (cùng seed -> cùng data) — cần cho reconciliation test
# ở Phase 4. Không set thì mỗi lần chạy ra data khác.
_SEED = os.getenv("RANDOM_SEED")
if _SEED:
    random.seed(int(_SEED))
    Faker.seed(int(_SEED))

# ---- Config (env) -----------------------------------------------------------
DSN = (
    f"host={os.getenv('PGHOST', 'postgres')} "
    f"port={os.getenv('PGPORT', '5432')} "
    f"user={os.getenv('PGUSER', 'bank')} "
    f"password={os.getenv('PGPASSWORD', 'bank')} "
    f"dbname={os.getenv('PGDATABASE', 'banking')}"
)
TXN_RATE = int(os.getenv("TXN_RATE", "20"))   # INSERT/giây mục tiêu
N_ACCOUNTS = int(os.getenv("N_ACCOUNTS", "100"))
N_MERCHANTS = int(os.getenv("N_MERCHANTS", "50"))
# Tuổi tối thiểu để một giao dịch FAILED bị purge. Chính sách THẬT tính bằng
# tháng/năm; demo NÉN THANG THỜI GIAN xuống phút để hành vi quan sát được trong
# một phiên làm việc. Logic y hệt ở mọi quy mô, chỉ khác con số.
PURGE_AFTER_MINUTES = int(os.getenv("PURGE_AFTER_MINUTES", "2"))

# ---- Mô phỏng SỰ CỐ (cố ý, không phải bug) ----------------------------------
# Chọn theo txn_id thay vì random() để nhóm treo là XÁC ĐỊNH: một dòng đã treo thì
# treo thật, không phải mỗi vòng lặp lại được tung xúc xắc rồi vô tình được cứu.
STUCK_EVERY = int(os.getenv("STUCK_EVERY", "200"))            # 1/200 giao dịch bị treo
UNRECOVERABLE_EVERY = int(os.getenv("UNRECOVERABLE_EVERY", "1000"))  # 1/1000 treo vĩnh viễn
# quá hạn này thì job quét vào cuộc
STUCK_AFTER_MINUTES = int(os.getenv("STUCK_AFTER_MINUTES", "3"))
# Giao dịch đã timeout thì XÁC SUẤT THÀNH CÔNG THẤP HƠN giao dịch bình thường —
# hệ thống thật xác định kết quả bằng cách hỏi lại mạng thanh toán, không phải tung
# đồng xu; con số này chỉ để dữ liệu phản ánh đúng xu hướng đó.
SWEEP_COMPLETE_RATE = 0.5

ACCOUNT_TYPES = ["SAVINGS", "CHECKING", "CREDIT"]
TXN_TYPES = ["CREDIT", "DEBIT", "TRANSFER"]
CHANNELS = ["ATM", "POS", "ONLINE", "MOBILE", "BRANCH"]
CATEGORIES = ["Grocery", "Fuel", "Dining", "Retail", "Utilities", "Travel", "Health"]


def connect_with_retry(max_wait=60):
    """Đợi Postgres sẵn sàng (compose depends_on healthy đã lo, nhưng phòng hờ)."""
    start = time.time()
    while True:
        try:
            conn = psycopg.connect(DSN, autocommit=True)
            print(">> Connected to Postgres", flush=True)
            return conn
        except psycopg.OperationalError as e:
            if time.time() - start > max_wait:
                raise
            print(f".. waiting for Postgres ({e})", flush=True)
            time.sleep(2)


def seed(conn):
    """Seed accounts + merchants MỘT LẦN (idempotent: bỏ qua nếu đã có data)."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts")
        if cur.fetchone()[0] > 0:
            print(">> Seed skipped (accounts already present)", flush=True)
            return

        merchants = [
            (fake.company(), random.choice(CATEGORIES), fake.city())
            for _ in range(N_MERCHANTS)
        ]
        cur.executemany(
            "INSERT INTO merchants (merchant_name, category, city) VALUES (%s, %s, %s)",
            merchants,
        )

        accounts = [
            (
                fake.name(), fake.ssn(), fake.phone_number(),
                fake.email(), fake.date_of_birth(minimum_age=18, maximum_age=90),
                random.choice(ACCOUNT_TYPES),
                decimal.Decimal(random.randrange(0, 5_000_000)) / 100,  # 0..50,000.00
            )
            for _ in range(N_ACCOUNTS)
        ]
        cur.executemany(
            """INSERT INTO accounts
               (customer_name, national_id, phone, email, date_of_birth, account_type, balance)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            accounts,
        )
        print(f">> Seeded {N_MERCHANTS} merchants, {N_ACCOUNTS} accounts", flush=True)


def load_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM accounts")
        accs = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT merchant_id FROM merchants")
        mers = [r[0] for r in cur.fetchall()]
    return accs, mers


def insert_pending(cur, accs, mers):
    """INSERT 1 giao dịch PENDING mới (op=c cho CDC)."""
    amount = decimal.Decimal(random.randrange(1, 1_000_000)) / 100  # 0.01..10,000.00
    cur.execute(
        """INSERT INTO transactions
           (account_id, merchant_id, amount, currency, txn_type, status, channel)
           VALUES (%s, %s, %s, 'USD', %s, 'PENDING', %s)""",
        (random.choice(accs), random.choice(mers), amount,
         random.choice(TXN_TYPES), random.choice(CHANNELS)),
    )


def balance_delta(txn_type, amount):
    """Chiều tiền theo loại giao dịch: CREDIT = tiền vào (+), DEBIT/TRANSFER = tiền ra (-).

    TRANSFER hiện mô hình như dòng tiền RA khỏi account khởi tạo (schema chưa có cột
    tài khoản đích). Chuyển khoản liên tài khoản đúng nghĩa cần thêm counterparty_account_id
    — ghi nhận là cải tiến schema về sau (issue).
    """
    return amount if txn_type == "CREDIT" else -amount


def advance_pending(cur, limit=10):
    """UPDATE: PENDING -> COMPLETED (85%) / FAILED (15%). Sinh op=u (before/after status).

    Chỉ COMPLETED mới CHUYỂN TIỀN THẬT -> cập nhật accounts.balance (sinh thêm CDC UPDATE
    trên bảng dimension accounts). FAILED không đụng tiền (đúng logic nghiệp vụ: chỉ
    COMPLETED = tiền thật). updated_at do trigger DB tự set.

    Loại trừ nhóm TREO (txn_id chia hết cho STUCK_EVERY): chúng cố tình không được
    vòng đời bình thường xử lý, để tích lại thành giao dịch in-doubt. Phải loại trừ
    ngay trong câu SELECT, nếu không thì các dòng treo (cũ nhất) sẽ chiếm hết LIMIT
    và chặn đứng việc xử lý các giao dịch bình thường.
    """
    cur.execute(
        "SELECT txn_id, account_id, amount, txn_type FROM transactions "
        "WHERE status = 'PENDING' AND txn_id %% %s <> 0 "
        "ORDER BY created_at LIMIT %s", (STUCK_EVERY, limit),
    )
    for txn_id, account_id, amount, txn_type in cur.fetchall():
        completed = random.random() < 0.85
        new_status = "COMPLETED" if completed else "FAILED"
        cur.execute(
            "UPDATE transactions SET status = %s WHERE txn_id = %s",
            (new_status, txn_id),
        )
        if completed:
            cur.execute(
                "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                (balance_delta(txn_type, amount), account_id),
            )


def reverse_some(cur, limit=1):
    """REVERSAL: COMPLETED -> REVERSED (hiếm). HOÀN LẠI đúng phần đã cộng/trừ vào balance.

    Nền để Gold (Phase 2) test: reversal không đếm hai lần, net amount phản ánh đảo chiều.
    """
    cur.execute(
        "SELECT txn_id, account_id, amount, txn_type FROM transactions "
        "WHERE status = 'COMPLETED' ORDER BY random() LIMIT %s", (limit,),
    )
    for txn_id, account_id, amount, txn_type in cur.fetchall():
        cur.execute(
            "UPDATE transactions SET status = 'REVERSED' WHERE txn_id = %s", (txn_id,),
        )
        cur.execute(   # đảo chiều dòng tiền
            "UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
            (balance_delta(txn_type, amount), account_id),
        )


def sweep_stuck_pending(cur, limit=5):
    """Job quét giao dịch TREO — cơ chế đóng cửa sổ in-doubt của hệ thống thật.

    BÀI TOÁN: server có thể chết SAU khi đã gọi mạng thanh toán nhưng TRƯỚC khi kịp
    ghi kết quả. DB nói PENDING, nhưng tiền có thể đã chuyển thật. Không ai biết.

    CÁCH XỬ LÝ THẬT: một job chạy nền tìm các PENDING quá hạn, HỎI LẠI mạng thanh
    toán xem thực tế ra sao, rồi chốt trạng thái. Ở đây không có mạng thanh toán thật
    nên kết quả được sinh ngẫu nhiên — nhưng với tỷ lệ thành công THẤP hơn giao dịch
    bình thường, vì giao dịch đã timeout thì khả năng hỏng cao hơn.

    CỐ Ý CHỪA LẠI: nhóm txn_id chia hết cho UNRECOVERABLE_EVERY KHÔNG bao giờ được
    job này cứu. Đó là các giao dịch cần con người can thiệp — và chính chúng là thứ
    phải nổi lên dashboard vận hành ở Phase 5. Nếu quét sạch không chừa dòng nào thì
    bảng nguồn lại trở về trạng thái "không bao giờ có sự cố", đúng cái ta đang tránh.
    """
    cur.execute(
        "SELECT txn_id, account_id, amount, txn_type FROM transactions "
        "WHERE status = 'PENDING' "
        "  AND created_at < now() - make_interval(mins => %s) "
        "  AND txn_id %% %s <> 0 "
        "ORDER BY created_at LIMIT %s",
        (STUCK_AFTER_MINUTES, UNRECOVERABLE_EVERY, limit),
    )
    rows = cur.fetchall()
    for txn_id, account_id, amount, txn_type in rows:
        completed = random.random() < SWEEP_COMPLETE_RATE
        cur.execute(
            "UPDATE transactions SET status = %s WHERE txn_id = %s",
            ("COMPLETED" if completed else "FAILED", txn_id),
        )
        if completed:   # chỉ COMPLETED mới động vào tiền, y như vòng đời bình thường
            cur.execute(
                "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                (balance_delta(txn_type, amount), account_id),
            )
    return len(rows)


def purge_failed(cur, limit=3):
    """DELETE giao dịch FAILED quá hạn lưu trữ. Sinh op=d + tombstone cho CDC.

    VÌ SAO CHỈ XOÁ 'FAILED':
    FAILED là giao dịch KHÔNG chuyển tiền (xem advance_pending: chỉ COMPLETED mới
    đụng balance). Nên xoá chúng không phá vỡ đối soát "tổng Bronze = tổng Gold" ở
    Phase 4. COMPLETED và REVERSED là TIỀN THẬT — không bao giờ được hard-delete;
    ngân hàng đảo chiều bằng REVERSAL chứ không xoá bản ghi.

    VÌ SAO CÓ HÀM NÀY:
    Không phải để test cho đẹp. Một hệ thống không bao giờ xoá gì mới là thứ phi
    thực tế — chính sách lưu trữ dữ liệu (và cả yêu cầu xoá theo luật bảo vệ dữ
    liệu cá nhân) đều sinh ra DELETE thật. Quan trọng hơn: đây thường là JOB CHẠY
    NGOÀI ứng dụng, đúng loại thay đổi mà CDC log-based bắt được còn polling theo
    timestamp thì BỎ SÓT HOÀN TOÀN — vì dòng đã biến mất, không còn timestamp nào
    để so sánh. Đây là luận điểm trung tâm của cả pipeline này.
    """
    cur.execute(
        """DELETE FROM transactions
           WHERE txn_id IN (
               SELECT txn_id FROM transactions
               WHERE status = 'FAILED'
                 AND created_at < now() - make_interval(mins => %s)
               ORDER BY created_at
               LIMIT %s
           )""",
        (PURGE_AFTER_MINUTES, limit),
    )
    return cur.rowcount


def main():
    conn = connect_with_retry()
    seed(conn)
    accs, mers = load_ids(conn)

    interval = 1.0 / max(TXN_RATE, 1)
    tick = 0
    purged_total = 0
    swept_total = 0
    print(f">> Generating ~{TXN_RATE} txn/s (INSERT) + UPDATE lifecycle + occasional REVERSAL"
          f" + purge FAILED >{PURGE_AFTER_MINUTES}m"
          f" | 1/{STUCK_EVERY} txn treo, sweeper cứu sau {STUCK_AFTER_MINUTES}m,"
          f" 1/{UNRECOVERABLE_EVERY} treo vĩnh viễn",
          flush=True)
    while True:
        with conn.cursor() as cur:
            insert_pending(cur, accs, mers)          # mỗi tick: 1 INSERT
            if tick % 5 == 0:                        # định kỳ đẩy PENDING tiến trạng thái
                advance_pending(cur, limit=8)
            if tick % 50 == 0:                       # thi thoảng đảo chiều 1 giao dịch
                reverse_some(cur, limit=1)
            if tick % 300 == 0:                      # job quét giao dịch treo quá hạn
                swept_total += sweep_stuck_pending(cur, limit=5)
            if tick % 500 == 0:                      # job dọn dẹp theo chính sách lưu trữ
                purged_total += purge_failed(cur, limit=3)
        tick += 1
        if tick % 200 == 0:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM transactions")
                total = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM transactions WHERE status = 'PENDING'")
                pending = cur.fetchone()[0]
            print(f".. {total} transactions so far "
                  f"({pending} pending, {swept_total} swept, {purged_total} purged)", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
