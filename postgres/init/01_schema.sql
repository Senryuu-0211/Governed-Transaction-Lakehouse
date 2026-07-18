-- =============================================================================
-- Banking source schema (OLTP) — Phase 1 Step 1a
-- =============================================================================
-- Chạy MỘT LẦN khi volume Postgres trống (docker-entrypoint-initdb.d).
-- Đổi schema về sau => phải `docker compose down -v` rồi `up` (init chỉ chạy trên
-- volume trống — bài học Project 1).
--
-- Đây là SOURCE database mà CDC (Debezium, Step 1b) sẽ đọc WAL để bắt mọi thay đổi.
-- Mọi lựa chọn kiểu dữ liệu ở đây là quyết định CORRECTNESS, không phải cho tiện.
-- =============================================================================

-- ---- ENUM types --------------------------------------------------------------
-- Dùng ENUM (không phải free-text) để ràng buộc miền giá trị ngay tại source —
-- data quality bắt đầu từ OLTP, không đợi tới lakehouse mới sửa.
CREATE TYPE account_type AS ENUM ('SAVINGS', 'CHECKING', 'CREDIT');
CREATE TYPE txn_type     AS ENUM ('CREDIT', 'DEBIT', 'TRANSFER');
-- Vòng đời trạng thái CÓ CHỦ ĐÍCH: PENDING -> COMPLETED/FAILED -> REVERSED.
-- Chính các bước chuyển này tạo ra UPDATE trên WAL để CDC bắt được (Step 1b),
-- và là nền để test logic đảo chiều (REVERSAL) ở Gold (Phase 2).
CREATE TYPE txn_status   AS ENUM ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED');
CREATE TYPE txn_channel  AS ENUM ('ATM', 'POS', 'ONLINE', 'MOBILE', 'BRANCH');

-- ---- merchants (dimension, ~50) ---------------------------------------------
CREATE TABLE merchants (
    merchant_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_name TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    city          TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---- accounts (dimension, ~100) — CHỨA PII ----------------------------------
-- 🔒 = PII. Các cột này sẽ được MASK ở tầng Silver (Phase 2/4). Đánh dấu ngay từ
-- source để lineage/PII governance sau này biết chính xác cột nào nhạy cảm.
CREATE TABLE accounts (
    account_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_name TEXT           NOT NULL,   -- 🔒 PII
    national_id   TEXT           NOT NULL,   -- 🔒 PII
    phone         TEXT           NOT NULL,   -- 🔒 PII
    email         TEXT           NOT NULL,   -- 🔒 PII
    date_of_birth DATE           NOT NULL,   -- 🔒 PII
    account_type  account_type   NOT NULL,
    -- Tiền LUÔN dùng DECIMAL, KHÔNG float: float không biểu diễn chính xác thập phân
    -- (0.1+0.2 != 0.3) — sai số tiền tệ là lỗi correctness không thể chấp nhận ở bank.
    balance       DECIMAL(15,2)  NOT NULL DEFAULT 0.00,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---- transactions (fact, volume cao) ----------------------------------------
CREATE TABLE transactions (
    txn_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  BIGINT       NOT NULL REFERENCES accounts(account_id),
    merchant_id BIGINT       NOT NULL REFERENCES merchants(merchant_id),
    amount      DECIMAL(15,2) NOT NULL CHECK (amount > 0),  -- độ chính xác tiền tệ
    currency    CHAR(3)      NOT NULL DEFAULT 'USD',        -- ISO 4217; DEFAULT trung tính
    txn_type    txn_type     NOT NULL,
    status      txn_status   NOT NULL DEFAULT 'PENDING',
    channel     txn_channel  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Index phục vụ faker cập nhật trạng thái nhanh (tìm PENDING/COMPLETED để chuyển tiếp).
CREATE INDEX idx_txn_status ON transactions(status);

-- ---- updated_at do DB tự set (trigger), KHÔNG để app tự set --------------------
-- Bất kỳ writer nào UPDATE hàng (faker, tay, hay app khác) đều được bump updated_at
-- một cách đáng tin cậy tại nguồn. Đây là correctness về thời gian mà CDC/Silver dựa
-- vào (vd tính độ trễ, chọn bản mới nhất theo updated_at). App set tay dễ sai/sót.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_txn_updated_at
    BEFORE UPDATE ON transactions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =============================================================================
-- REPLICA IDENTITY FULL — quyết định CORRECTNESS cho CDC
-- =============================================================================
-- Mặc định Postgres chỉ ghi PRIMARY KEY vào WAL cho UPDATE/DELETE. Debezium khi đó
-- KHÔNG có "before-image" của các cột non-PK (vd status cũ). Đặt FULL để WAL chứa
-- TOÀN BỘ hàng trước thay đổi -> Debezium phát được before/after đầy đủ (op=u, op=d),
-- điều kiện bắt buộc để so sánh thay đổi status và test REVERSAL đúng ở phase sau.
-- Đánh đổi: WAL to hơn — chấp nhận được ở quy mô demo; ở tầm bank thật sẽ cân nhắc
-- FULL chỉ cho bảng cần before-image.
ALTER TABLE accounts     REPLICA IDENTITY FULL;
ALTER TABLE merchants    REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
