"""Data faker cho banking source DB.

Ba phần:
  1. SEED      — merchants + accounts (một lần, idempotent)
  2. BACKFILL  — nạp 90 ngày LỊCH SỬ bằng COPY, để có cái mà so sánh kỳ
  3. LIVE      — vòng lặp sinh thay đổi liên tục cho CDC bắt

Cố tình trộn 4 loại thao tác để phủ mọi op của Debezium:
  - INSERT  : giao dịch mới PENDING                          -> op=c
  - UPDATE  : PENDING -> COMPLETED/FAILED, rồi REVERSED      -> op=u (before/after)
  - DELETE  : purge giao dịch FAILED quá hạn lưu trữ         -> op=d (+ tombstone)

KHÔNG mô phỏng một thế giới hoàn hảo — bảng nguồn chứa sẵn ca xấu (giao dịch treo,
retry trùng khoá, sự cố merchant/kênh, chiến dịch dò thẻ). Nếu dữ liệu không bao
giờ có ca xấu thì logic xử lý ca xấu ở Silver/Gold/alerting KHÔNG BAO GIỜ được
chạy, sai mà không ai phát hiện.

VÌ SAO CÓ BACKFILL (09-2026):
Bản cũ chỉ sinh dữ liệu từ "bây giờ" trở đi, nên sau vài ngày chạy mart chỉ có 6
dòng. Không so sánh được kỳ, không có nền cho anomaly test, và câu hỏi "vì sao kỳ
này giảm?" không có đáp án vì không có kỳ nào trước đó.

Mọi giả định về hình dạng thị trường (persona, địa lý, ngành hàng, sự cố) nằm ở
`world.py` — tách ra để CI unit-test được mà không cần DB.

Chỉ dùng dữ liệu SYNTHETIC (Faker) — KHÔNG bao giờ PII thật.
"""
import itertools
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import world

from faker import Faker

fake = Faker()

_SEED = os.getenv("RANDOM_SEED")
RNG = random.Random(int(_SEED) if _SEED else None)
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
# float, không int: nhịp live PHẢI khớp mật độ backfill. 20/giây = 1,7 triệu giao
# dịch/ngày trong khi lịch sử chỉ 100.000/ngày -> 17 phút chạy live là bằng cả một
# ngày lịch sử, và "hôm nay" vĩnh viễn là ngoại lệ khổng lồ làm hỏng mọi phép so
# sánh kỳ. 1,2/giây ~= 104.000/ngày, khớp với backfill.
TXN_RATE = float(os.getenv("TXN_RATE", "1.2"))
# 25.000 tài khoản cho 100.000 giao dịch/ngày ~= 4 giao dịch/ngày/tài khoản hoạt
# động. Ở mức 2.000 tài khoản thì thành 50/ngày/tài khoản — không ai tiêu như vậy,
# và "xếp hạng khách hàng" cũng vô nghĩa khi chỉ có hai nghìn.
N_ACCOUNTS = int(os.getenv("N_ACCOUNTS", "25000"))
N_MERCHANTS = int(os.getenv("N_MERCHANTS", "200"))

# Backfill: 90 ngày × 100.000 ≈ 9 triệu giao dịch, COPY mất ~7 phút.
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "90"))
BACKFILL_TXN_PER_DAY = int(os.getenv("BACKFILL_TXN_PER_DAY", "100000"))

PURGE_AFTER_MINUTES = int(os.getenv("PURGE_AFTER_MINUTES", "2"))

# ---- Mô phỏng SỰ CỐ ở tầng ứng dụng (cố ý, không phải bug) -------------------
# Chọn theo txn_id thay vì random() để nhóm treo là XÁC ĐỊNH: một dòng đã treo thì
# treo thật, không phải mỗi vòng lặp lại được tung xúc xắc rồi vô tình được cứu.
STUCK_EVERY = int(os.getenv("STUCK_EVERY", "200"))
UNRECOVERABLE_EVERY = int(os.getenv("UNRECOVERABLE_EVERY", "1000"))
STUCK_AFTER_MINUTES = int(os.getenv("STUCK_AFTER_MINUTES", "3"))
SWEEP_COMPLETE_RATE = 0.5

ACCOUNT_TYPES = ["SAVINGS", "CHECKING", "CREDIT"]
REVERSAL_RATE = 0.008          # ~0,8% giao dịch COMPLETED bị đảo chiều
STUCK_PENDING_RATE = 0.004     # ~0,4% lịch sử còn treo PENDING vĩnh viễn

# Giờ cao điểm: giao dịch ngân hàng dồn vào giờ hành chính và đầu buổi tối.
HOUR_WEIGHTS = [
    1, 1, 1, 1, 1, 2, 4, 7, 12, 16, 18, 17,
    14, 15, 17, 18, 17, 16, 14, 11, 8, 5, 3, 2,
]

COPY_COLS = ("account_id, merchant_id, counterparty_account_id, idempotency_key, "
             "amount, currency, txn_type, status, channel, is_flagged, fraud_label, "
             "created_at, updated_at")

_recent_keys: list[str] = []   # mô phỏng client retry (issue #004)


def connect_with_retry(max_wait=60):
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


# =============================================================================
# 1. SEED
# =============================================================================
def seed(conn):
    """Seed merchants + accounts MỘT LẦN (idempotent: bỏ qua nếu đã có data)."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM accounts")
        if cur.fetchone()[0] > 0:
            print(">> Seed skipped (accounts already present)", flush=True)
            return False

        # Ngành hàng gán TẤT ĐỊNH theo trọng số (xem assign_merchant_categories),
        # không rút ngẫu nhiên — nếu không thì vài merchant lớn rơi cùng ngành là
        # ngành đó nuốt 40% doanh số còn ngành khác còn 1%.
        cats = world.assign_merchant_categories(world.build_merchant_weights(N_MERCHANTS))
        merchants = []
        for cat in cats:
            region, city = world.pick_region(RNG)
            merchants.append((fake.company(), cat, region, city))
        cur.executemany(
            "INSERT INTO merchants (merchant_name, category, region, city) "
            "VALUES (%s, %s, %s, %s)", merchants)

        personas = world.assign_personas(N_ACCOUNTS, RNG)
        accounts = []
        for persona in personas:
            region, _city = world.pick_region(RNG)
            accounts.append((
                fake.name(), fake.ssn(), fake.phone_number(), fake.email(),
                fake.date_of_birth(minimum_age=18, maximum_age=90),
                random.choice(ACCOUNT_TYPES), persona, region,
                # Số dư mở đầu; backfill cộng/trừ lịch sử giao dịch lên trên.
                world.to_decimal(random.randrange(100_00, 5_000_000)),
            ))
        cur.executemany(
            """INSERT INTO accounts
               (customer_name, national_id, phone, email, date_of_birth,
                account_type, persona, home_region, balance)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", accounts)
        print(f">> Seeded {N_MERCHANTS} merchants, {N_ACCOUNTS} accounts", flush=True)
        return True


def load_world(conn):
    """Nạp lại thế giới từ DB để backfill và luồng live nhìn thấy CÙNG một thứ."""
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, persona FROM accounts ORDER BY account_id")
        rows = cur.fetchall()
        accs = [r[0] for r in rows]
        personas = [r[1] for r in rows]
        cur.execute("SELECT merchant_id, category FROM merchants ORDER BY merchant_id")
        mrows = cur.fetchall()
    return accs, personas, [m[0] for m in mrows], [m[1] for m in mrows]


# =============================================================================
# 2. BACKFILL
# =============================================================================
class Draw:
    """Bộ rút mẫu có trọng số, dựng MỘT LẦN.

    `random.choices(..., weights=...)` tính lại tổng tích luỹ ở MỖI lần gọi — O(n).
    Với 25.000 tài khoản × 9 triệu lượt rút thì không bao giờ xong. Dùng
    `cum_weights` dựng sẵn thì mỗi lần rút chỉ là một phép tìm nhị phân, và rút cả
    lô k phần tử một lần nữa.
    """

    def __init__(self, accs, personas, mers, rng):
        self.rng = rng
        self.accs = accs
        self.mers = mers
        self.acc_cum = list(itertools.accumulate(
            world.account_activity_weights(personas)))
        # KHÔNG truyền rng: dùng seed cố định để khớp đúng bộ trọng số mà seed()
        # đã dùng khi gán ngành hàng.
        mw = world.build_merchant_weights(len(mers))
        self.mer_cum = list(itertools.accumulate(mw))
        # Phiên bản LOẠI merchant top, dùng trong những ngày nó ngừng hoạt động.
        # Rẻ hơn hẳn so với rút rồi loại bỏ và rút lại.
        self.mer_cum_no0 = list(itertools.accumulate([0.0] + mw[1:]))
        self.weights = mw

    def accounts(self, k):
        return self.rng.choices(range(len(self.accs)), cum_weights=self.acc_cum, k=k)

    def merchants(self, k, top_is_down):
        cum = self.mer_cum_no0 if top_is_down else self.mer_cum
        return self.rng.choices(range(len(self.mers)), cum_weights=cum, k=k)


def _ts(day, rng, hour=None, max_hour=None):
    """Dấu thời gian trong ngày, theo nhịp giờ cao điểm.

    `max_hour` (issue #007): backfill giờ phủ CẢ HÔM NAY để không còn lỗ ở ngày giao
    nhau giữa lịch sử và luồng live. Nhưng hôm nay mới trôi được vài giờ, nên phải
    chặn trần — sinh giao dịch ở 23:00 trong khi mới 05:00 là dữ liệu từ TƯƠNG LAI,
    và mọi phép so sánh theo thời gian sẽ lệch mà không báo lỗi.
    """
    if hour is None:
        w = HOUR_WEIGHTS if max_hour is None else HOUR_WEIGHTS[:max_hour + 1]
        hour = rng.choices(range(len(w)), weights=w, k=1)[0]
    elif max_hour is not None and hour > max_hour:
        hour = max_hour
    return datetime(day.year, day.month, day.day, hour,
                    rng.randrange(60), rng.randrange(60), tzinfo=timezone.utc)


def _normal_row(day, today, draw, personas, categories, acc_i, mer_i, rng, max_hour=None):
    """Một giao dịch lịch sử BÌNH THƯỜNG, ở TRẠNG THÁI CUỐI.

    Lịch sử không diễn lại vòng đời PENDING->COMPLETED: làm vậy cần thêm ~5 triệu
    UPDATE và mất hàng giờ. Đánh đổi có chủ đích — dòng lịch sử chỉ mang op=r/c,
    còn vòng đời đầy đủ (op=u, op=d) do luồng LIVE sinh ra liên tục.
    """
    persona = personas[acc_i]
    category = categories[mer_i]
    channel = world.pick_channel(persona, rng)
    amount = world.to_decimal(world.amount_cents(persona, category, day, today, rng))

    if rng.random() < world.TRANSFER_SHARE:
        txn_type = "TRANSFER"
        cp_i = rng.randrange(len(draw.accs))
        while cp_i == acc_i:
            cp_i = rng.randrange(len(draw.accs))
        counterparty = draw.accs[cp_i]
    else:
        txn_type = rng.choice(["CREDIT", "DEBIT"])
        counterparty = None

    fail_rate = world.channel_failure_rate(channel, day, today)
    r = rng.random()
    if r < fail_rate:
        status = "FAILED"
    elif r < fail_rate + STUCK_PENDING_RATE:
        status = "PENDING"
    elif rng.random() < REVERSAL_RATE:
        status = "REVERSED"
    else:
        status = "COMPLETED"

    is_fraud = rng.random() < world.FRAUD_BASE_RATE
    created = _ts(day, rng, max_hour=max_hour)
    # issue #007: chuyển khoản nội bộ KHÔNG thuộc về merchant nào. Vẫn rút mer_i ở
    # trên (nó quyết định khoảng tiền) nhưng KHÔNG ghi xuống — ghi xuống là bắt dữ
    # liệu nói dối, và mọi ngành hàng bị thổi thêm ~15% giao dịch chưa từng xảy ra.
    merchant = None if txn_type == "TRANSFER" else draw.mers[mer_i]
    return (draw.accs[acc_i], merchant, counterparty, str(uuid.uuid4()),
            amount, "USD", txn_type, status, channel,
            world.fraud_detected(is_fraud, rng), is_fraud,
            created, created + timedelta(seconds=rng.randrange(1, 900)))


def _salary_rows(day, draw, personas, rng, max_hour=None):
    """Lương về: một SALARY cho mỗi tài khoản nhận lương.

    ⚠️ issue #007 — bản cũ gán `draw.mers[0]` (merchant LỚN NHẤT) cho mọi khoản lương,
    nên toàn bộ lương của 25.000 tài khoản đổ vào một cửa hàng Grocery: 99.041 dòng,
    8,5% khối lượng của merchant đó. Nó còn không hỏi merchant có đang hoạt động
    không, nên SỰ CỐ #1 (merchant tắt) bị nhiễu — mà sự cố đó là ĐÁP ÁN chấm điểm
    agent. Giờ lương không có merchant, và mang loại riêng.

    Nhịp "ngày lương" vì vậy NỔI LÊN TỪ DỮ LIỆU — có giao dịch thật đứng sau —
    thay vì là một hệ số nhân gắn tay như bản trước. Khác biệt này quan trọng khi
    ai đó hỏi "con số này từ đâu ra".
    """
    for i, persona in enumerate(personas):
        if not world.PERSONAS[persona]["salaried"]:
            continue
        created = _ts(day, rng, hour=rng.choice([8, 9, 10]), max_hour=max_hour)
        yield (draw.accs[i], None, None, str(uuid.uuid4()),
               world.to_decimal(world.salary_cents(rng)), "USD", "SALARY",
               "COMPLETED", "BRANCH", False, False, created, created)


def _fraud_burst_rows(day, draw, categories, rng):
    """Chiến dịch DÒ THẺ: nhiều giao dịch rất nhỏ, dồn trong ít phút, phần lớn hỏng.

    Cấy theo CẤU TRÚC chứ không phải theo phân phối — ở tỷ lệ gian lận thật (~0,1%)
    thì một mẫu chỉ lệch nhẹ về giờ giấc là KHÔNG AI tìm ra, kể cả người lẫn máy
    (dự án `fake-banking-data` tự thừa nhận điều đó). Cấu trúc thì tìm được, và
    quan trọng hơn: CHỨNG MINH được agent tìm đúng hay sai.
    """
    for acc_i in draw.accounts(world.FRAUD_BURST_ACCOUNTS_PER_DAY):
        n = rng.randint(*world.FRAUD_BURST_SIZE)
        base = _ts(day, rng, hour=rng.randrange(1, 5))   # giờ vắng
        for j in range(n):
            mer_i = draw.merchants(1, False)[0]
            failed = rng.random() < world.FRAUD_BURST_FAIL_RATE
            created = base + timedelta(seconds=j * rng.randrange(8, 20))
            yield (draw.accs[acc_i], draw.mers[mer_i], None, str(uuid.uuid4()),
                   world.to_decimal(rng.randint(*world.FRAUD_BURST_AMOUNT)), "USD",
                   "DEBIT", "FAILED" if failed else "COMPLETED",
                   rng.choice(["ONLINE", "POS"]),
                   world.fraud_detected(True, rng), True,
                   created, created)
            _ = categories  # ngành hàng không ảnh hưởng số tiền dò thẻ


def backfill(conn, draw, personas, categories):
    """Nạp lịch sử bằng COPY. executemany cho 9 triệu dòng là hàng giờ."""
    if BACKFILL_DAYS <= 0:
        print(">> Backfill tắt (BACKFILL_DAYS=0)", flush=True)
        return
    now = datetime.now(timezone.utc)
    today = now.date()
    rng = RNG
    total = fraud_total = 0
    t0 = time.time()
    print(f">> Backfill {BACKFILL_DAYS} ngày × ~{BACKFILL_TXN_PER_DAY:,} giao dịch...",
          flush=True)

    # Tỷ lệ của ngày hôm nay đã trôi qua, tính theo NHỊP GIỜ chứ không theo đồng hồ:
    # 06:00 không phải 25% sản lượng một ngày, vì ban đêm gần như không có giao dịch.
    hours_total = sum(HOUR_WEIGHTS)
    today_share = sum(HOUR_WEIGHTS[:now.hour]) / hours_total

    with conn.cursor() as cur:
        # include_today (issue #007): phủ tới TẬN BÂY GIỜ. Bản cũ dừng ở hôm qua còn
        # luồng live bắt đầu lúc khởi động -> ngày giao nhau có lỗ thật nhiều tiếng,
        # và cổng assert_rowcount_not_dropped chặn push_marts tới tận nửa đêm.
        for day in world.date_range(today, BACKFILL_DAYS, include_today=True):
            is_today = day == today
            share = today_share if is_today else 1.0
            # `now.hour - 1`, KHÔNG phải `now.hour`: backfill chỉ lấp các giờ ĐÃ XONG,
            # giờ đang diễn ra để luồng live lấp. Dùng now.hour thì sinh ra giao dịch
            # ở phút bất kỳ của giờ hiện tại -> tới 59 phút TƯƠNG LAI, đúng thứ
            # max_hour sinh ra để chặn. Nó cũng khớp lại với today_share ở trên, vốn
            # cộng HOUR_WEIGHTS[:now.hour] = các giờ đã xong.
            max_hour = now.hour - 1 if is_today else None
            n = int(BACKFILL_TXN_PER_DAY * world.day_volume_factor(day, today) * share)
            if n <= 0:
                continue
            down = world.merchant_is_down(0, day, today)
            acc_idx = draw.accounts(n)
            mer_idx = draw.merchants(n, down)

            with cur.copy(f"COPY transactions ({COPY_COLS}) FROM STDIN") as cp:
                for a, m in zip(acc_idx, mer_idx, strict=True):
                    cp.write_row(_normal_row(day, today, draw, personas,
                                             categories, a, m, rng,
                                             max_hour=max_hour))
                if world.is_payday(day):
                    for row in _salary_rows(day, draw, personas, rng,
                                            max_hour=max_hour):
                        cp.write_row(row)
                        total += 1
                if world.fraud_burst_active(day, today):
                    for row in _fraud_burst_rows(day, draw, categories, rng):
                        cp.write_row(row)
                        fraud_total += 1
            total += n
            if day.day == 1:
                print(f"   .. {day} — {total:,} dòng", flush=True)

        # Số dư: tính MỘT LẦN từ tổng hợp thay vì UPDATE từng dòng.
        # Chỉ COMPLETED mới chuyển tiền thật — đúng luật của cả pipeline.
        print(">> Tính lại số dư từ lịch sử...", flush=True)
        cur.execute("""
            UPDATE accounts a SET balance = a.balance + t.delta
            FROM (SELECT account_id,
                         sum(CASE WHEN txn_type = 'CREDIT' THEN amount ELSE -amount END) AS delta
                  FROM transactions WHERE status = 'COMPLETED' GROUP BY account_id) t
            WHERE a.account_id = t.account_id
        """)
        # Tiền VÀO tài khoản đích của chuyển khoản (issue #002 — trước đây tiền bị
        # trừ ở nguồn mà không cộng vào đâu cả).
        cur.execute("""
            UPDATE accounts a SET balance = a.balance + t.amt
            FROM (SELECT counterparty_account_id AS aid, sum(amount) AS amt
                  FROM transactions
                  WHERE status = 'COMPLETED' AND txn_type = 'TRANSFER'
                  GROUP BY counterparty_account_id) t
            WHERE a.account_id = t.aid
        """)
    print(f">> Backfill xong: {total:,} giao dịch ({fraud_total:,} gian lận) "
          f"trong {time.time() - t0:.0f}s", flush=True)


# =============================================================================
# 3. LIVE
# =============================================================================
def insert_pending(cur, draw, personas, categories):
    """INSERT 1 giao dịch PENDING mới (op=c).

    Mô phỏng CLIENT RETRY (issue #004): cứ 1/RETRY_EVERY lần, client gửi lại đúng
    khoá idempotency cũ. DB từ chối bằng UNIQUE — và đó là HÀNH VI ĐÚNG, không phải
    lỗi: nó chứng minh hàng rào chống trừ tiền hai lần đang hoạt động.
    """
    rng = RNG
    today = datetime.now(timezone.utc).date()
    acc_i = draw.accounts(1)[0]
    mer_i = draw.merchants(1, world.merchant_is_down(0, today, today))[0]
    persona = personas[acc_i]
    category = categories[mer_i]
    channel = world.pick_channel(persona, rng)
    amount = world.to_decimal(world.amount_cents(persona, category, today, today, rng))

    if rng.random() < world.TRANSFER_SHARE:
        txn_type = "TRANSFER"
        cp_i = rng.randrange(len(draw.accs))
        while cp_i == acc_i:
            cp_i = rng.randrange(len(draw.accs))
        counterparty = draw.accs[cp_i]
    else:
        txn_type = rng.choice(["CREDIT", "DEBIT"])
        counterparty = None

    key = str(uuid.uuid4())
    retried = False
    if _recent_keys and rng.randrange(world.RETRY_EVERY) == 0:
        key = rng.choice(_recent_keys)
        retried = True

    try:
        cur.execute(
            """INSERT INTO transactions
               (account_id, merchant_id, counterparty_account_id, idempotency_key,
                amount, currency, txn_type, status, channel)
               VALUES (%s, %s, %s, %s, %s, 'USD', %s, 'PENDING', %s)""",
            (draw.accs[acc_i],
             # issue #007 — chuyển khoản nội bộ không thuộc merchant nào (CHECK ở
             # nguồn cũng chặn). Luồng live và backfill phải khớp nhau ở điểm này,
             # nếu không "hôm nay" mang hình dạng khác mọi ngày lịch sử.
             None if txn_type == "TRANSFER" else draw.mers[mer_i],
             counterparty, key, amount, txn_type, channel))
    except psycopg.errors.UniqueViolation:
        return "retry_blocked" if retried else "collision"

    _recent_keys.append(key)
    if len(_recent_keys) > 500:
        _recent_keys.pop(0)
    return "inserted"


def balance_delta(txn_type, amount):
    """Chiều tiền: CREDIT/SALARY = vào (+), DEBIT/TRANSFER = ra (-) khỏi account khởi tạo."""
    return amount if txn_type in ("CREDIT", "SALARY") else -amount


def _settle(cur, txn_id, account_id, counterparty, amount, txn_type, completed):
    """Chốt trạng thái + chuyển tiền nếu COMPLETED.

    TRANSFER hoàn tất sinh HAI UPDATE trên accounts (trừ nguồn, cộng đích) -> hai
    CDC event, đúng như một lệnh chuyển khoản thật.
    """
    cur.execute("UPDATE transactions SET status = %s WHERE txn_id = %s",
                ("COMPLETED" if completed else "FAILED", txn_id))
    if not completed:
        return
    cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                (balance_delta(txn_type, amount), account_id))
    if txn_type == "TRANSFER" and counterparty is not None:
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                    (amount, counterparty))


def advance_pending(cur, limit=10):
    """UPDATE: PENDING -> COMPLETED / FAILED (op=u với before/after status).

    Loại trừ nhóm TREO (txn_id chia hết cho STUCK_EVERY) ngay trong câu SELECT —
    nếu không, các dòng treo (cũ nhất) sẽ chiếm hết LIMIT và chặn đứng việc xử lý
    giao dịch bình thường.
    """
    cur.execute(
        "SELECT txn_id, account_id, counterparty_account_id, amount, txn_type, channel "
        "FROM transactions WHERE status = 'PENDING' AND txn_id %% %s <> 0 "
        "ORDER BY created_at LIMIT %s", (STUCK_EVERY, limit))
    today = datetime.now(timezone.utc).date()
    for txn_id, acc, cp, amount, txn_type, channel in cur.fetchall():
        ok = random.random() >= world.channel_failure_rate(channel, today, today)
        _settle(cur, txn_id, acc, cp, amount, txn_type, ok)


def reverse_some(cur, limit=1):
    """REVERSAL: COMPLETED -> REVERSED. HOÀN LẠI đúng phần đã cộng/trừ."""
    cur.execute(
        "SELECT txn_id, account_id, counterparty_account_id, amount, txn_type "
        "FROM transactions WHERE status = 'COMPLETED' ORDER BY random() LIMIT %s",
        (limit,))
    for txn_id, acc, cp, amount, txn_type in cur.fetchall():
        cur.execute("UPDATE transactions SET status = 'REVERSED' WHERE txn_id = %s",
                    (txn_id,))
        cur.execute("UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
                    (balance_delta(txn_type, amount), acc))
        if txn_type == "TRANSFER" and cp is not None:
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
                        (amount, cp))


def sweep_stuck_pending(cur, limit=5):
    """Job quét giao dịch TREO — cơ chế đóng cửa sổ in-doubt của hệ thống thật.

    BÀI TOÁN: server có thể chết SAU khi đã gọi mạng thanh toán nhưng TRƯỚC khi kịp
    ghi kết quả. DB nói PENDING, nhưng tiền có thể đã chuyển thật.

    CỐ Ý CHỪA LẠI nhóm chia hết cho UNRECOVERABLE_EVERY: đó là các giao dịch cần
    con người can thiệp, và chính chúng phải nổi lên dashboard vận hành.
    """
    cur.execute(
        "SELECT txn_id, account_id, counterparty_account_id, amount, txn_type "
        "FROM transactions WHERE status = 'PENDING' "
        "  AND created_at < now() - make_interval(mins => %s) "
        "  AND txn_id %% %s <> 0 ORDER BY created_at LIMIT %s",
        (STUCK_AFTER_MINUTES, UNRECOVERABLE_EVERY, limit))
    rows = cur.fetchall()
    for txn_id, acc, cp, amount, txn_type in rows:
        _settle(cur, txn_id, acc, cp, amount, txn_type,
                random.random() < SWEEP_COMPLETE_RATE)
    return len(rows)


def purge_failed(cur, limit=3):
    """DELETE giao dịch FAILED quá hạn lưu trữ -> op=d + tombstone.

    CHỈ xoá FAILED: nó không chuyển tiền nên xoá không phá vỡ đối soát. COMPLETED
    và REVERSED là TIỀN THẬT — ngân hàng đảo chiều bằng REVERSAL, không xoá bản ghi.

    Đây thường là JOB CHẠY NGOÀI ứng dụng — đúng loại thay đổi mà CDC log-based bắt
    được còn polling theo timestamp BỎ SÓT HOÀN TOÀN, vì dòng đã biến mất, không
    còn timestamp nào để so. Đây là luận điểm trung tâm của cả pipeline.
    """
    cur.execute(
        """DELETE FROM transactions WHERE txn_id IN (
               SELECT txn_id FROM transactions
               WHERE status = 'FAILED' AND created_at < now() - make_interval(mins => %s)
               ORDER BY created_at LIMIT %s)""",
        (PURGE_AFTER_MINUTES, limit))
    return cur.rowcount


def main():
    conn = connect_with_retry()
    fresh = seed(conn)
    accs, personas, mers, categories = load_world(conn)
    draw = Draw(accs, personas, mers, RNG)
    if fresh:
        backfill(conn, draw, personas, categories)

    interval = 1.0 / max(TXN_RATE, 0.01)
    tick = purged = swept = blocked = 0
    print(f">> LIVE: ~{TXN_RATE} txn/s (~{TXN_RATE * 86400:,.0f}/ngày)"
          f" + vòng đời + reversal + purge FAILED >{PURGE_AFTER_MINUTES}m"
          f" | 1/{STUCK_EVERY} treo · 1/{world.RETRY_EVERY} retry trùng khoá", flush=True)

    while True:
        with conn.cursor() as cur:
            if insert_pending(cur, draw, personas, categories) == "retry_blocked":
                blocked += 1
            if tick % 5 == 0:
                advance_pending(cur, limit=8)
            if tick % 50 == 0:
                reverse_some(cur, limit=1)
            if tick % 300 == 0:
                swept += sweep_stuck_pending(cur, limit=5)
            if tick % 500 == 0:
                purged += purge_failed(cur, limit=3)
        tick += 1
        if tick % 200 == 0:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*), count(*) FILTER (WHERE status = 'PENDING') "
                            "FROM transactions")
                total, pending = cur.fetchone()
            print(f".. {total:,} transactions ({pending} pending, {swept} swept, "
                  f"{purged} purged, {blocked} retry bị chặn)", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
