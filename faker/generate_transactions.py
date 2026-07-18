"""Data faker cho banking source DB — Phase 1 Step 1a.

Mục tiêu: tạo dòng thay đổi THẬT trên Postgres để CDC (Step 1b) có cái mà bắt.
Cố tình trộn 3 loại thao tác để bao phủ mọi op của Debezium về sau:
  - INSERT  : giao dịch mới ở trạng thái PENDING            -> CDC op=c
  - UPDATE  : PENDING -> COMPLETED/FAILED (vòng đời thật)    -> CDC op=u (before/after status)
  - REVERSAL: COMPLETED -> REVERSED (đảo chiều)              -> CDC op=u (nền test REVERSAL Gold)

Chỉ dùng dữ liệu SYNTHETIC (Faker) — KHÔNG bao giờ PII thật.
Tốc độ chỉnh qua env TXN_RATE (số INSERT/giây, mặc định 20).

Thiết kế để scale: logic giống nhau ở mọi quy mô; DEMO chạy nhỏ (~100 accounts,
~50 merchants, ~20 txn/s) trên home server — KHÔNG giả vờ chạy tỷ bản ghi.
"""
import os
import random
import time
import decimal

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
    """
    cur.execute(
        "SELECT txn_id, account_id, amount, txn_type FROM transactions "
        "WHERE status = 'PENDING' ORDER BY created_at LIMIT %s", (limit,),
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


def main():
    conn = connect_with_retry()
    seed(conn)
    accs, mers = load_ids(conn)

    interval = 1.0 / max(TXN_RATE, 1)
    tick = 0
    print(f">> Generating ~{TXN_RATE} txn/s (INSERT) + UPDATE lifecycle + occasional REVERSAL",
          flush=True)
    while True:
        with conn.cursor() as cur:
            insert_pending(cur, accs, mers)          # mỗi tick: 1 INSERT
            if tick % 5 == 0:                        # định kỳ đẩy PENDING tiến trạng thái
                advance_pending(cur, limit=8)
            if tick % 50 == 0:                       # thi thoảng đảo chiều 1 giao dịch
                reverse_some(cur, limit=1)
        tick += 1
        if tick % 200 == 0:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM transactions")
                total = cur.fetchone()[0]
            print(f".. {total} transactions so far", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
