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
-- SALARY tách riêng khỏi CREDIT (issue #007): lương KHÔNG phải là một khoản tiền vào
-- bất kỳ — nó không có merchant, và mọi tầng phía sau cần nhận ra điều đó bằng một
-- KHAI BÁO, không phải bằng suy đoán "CREDIT + BRANCH + ngày 1/15 + khoảng tiền".
-- Suy đoán thì âm thầm sai khi tham số mô phỏng đổi; khai báo thì không.
CREATE TYPE txn_type     AS ENUM ('CREDIT', 'DEBIT', 'TRANSFER', 'SALARY');
-- Vòng đời trạng thái CÓ CHỦ ĐÍCH: PENDING -> COMPLETED/FAILED -> REVERSED.
-- Chính các bước chuyển này tạo ra UPDATE trên WAL để CDC bắt được (Step 1b),
-- và là nền để test logic đảo chiều (REVERSAL) ở Gold (Phase 2).
CREATE TYPE txn_status   AS ENUM ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED');
CREATE TYPE txn_channel  AS ENUM ('ATM', 'POS', 'ONLINE', 'MOBILE', 'BRANCH');
-- Persona = ARCHETYPE HÀNH VI, không phải "khách to / khách nhỏ". Một khái niệm
-- chi phối cùng lúc: tần suất giao dịch, biên số tiền, kênh ưa dùng, có nhận lương
-- hay không. Đây là cách ngân hàng thật phân nhóm khách, và nó làm cho
-- "số tài khoản HOẠT ĐỘNG" trở thành con số biết di chuyển — nhân tử đầu tiên của
-- phép phân rã doanh số, thứ mà phân bố đều tuyệt đối không bao giờ cho ta.
CREATE TYPE customer_persona AS ENUM
    ('SALARIED', 'DIGITAL_NATIVE', 'TRADITIONAL', 'CORPORATE', 'DORMANT');

-- ---- merchants (dimension, ~50) ---------------------------------------------
CREATE TABLE merchants (
    merchant_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_name TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    -- Vùng gộp được, thành phố thì không: `fake.city()` cũ sinh ra hàng trăm thành
    -- phố đơn lẻ, mỗi cái một dòng -> không có chiều nào để tổng hợp. Vùng là chiều
    -- cắt tự nhiên nhất của mọi dashboard ngân hàng.
    region        TEXT        NOT NULL,
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
    persona       customer_persona NOT NULL,
    -- Vùng cư trú. Khác vùng của merchant = giao dịch ngoài vùng — một tín hiệu
    -- gian lận kinh điển, và ở đây nó có sẵn mà không tốn bảng nào.
    home_region   TEXT           NOT NULL,
    -- Tiền LUÔN dùng DECIMAL, KHÔNG float: float không biểu diễn chính xác thập phân
    -- (0.1+0.2 != 0.3) — sai số tiền tệ là lỗi correctness không thể chấp nhận ở bank.
    balance       DECIMAL(15,2)  NOT NULL DEFAULT 0.00,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---- transactions (fact, volume cao) ----------------------------------------
CREATE TABLE transactions (
    txn_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id  BIGINT       NOT NULL REFERENCES accounts(account_id),
    -- issue #007: NULL ĐƯỢC PHÉP. Hai loại giao dịch không thuộc về merchant nào —
    -- chuyển khoản nội bộ và lương. Ép chúng mang một merchant là bắt dữ liệu NÓI DỐI:
    -- một lần chuyển tiền cho bạn bị ghi là "Grocery, Metro North", và toàn bộ lương
    -- của 25.000 tài khoản dồn vào một siêu thị. Ràng buộc HAI CHIỀU ở dưới.
    merchant_id BIGINT       NULL     REFERENCES merchants(merchant_id),

    -- ---- issue #002: tài khoản ĐÍCH của chuyển khoản ------------------------
    -- Trước đây TRANSFER chỉ mô hình như dòng tiền RA (giống DEBIT), nên tiền
    -- "bốc hơi": trừ ở một bên mà không cộng vào đâu cả. Với một hệ thống mà
    -- mệnh đề trung tâm là "tiền vào = tiền ra", đó là lỗ hổng mô hình.
    -- NULL cho giao dịch với merchant; BẮT BUỘC có cho TRANSFER (xem CHECK dưới).
    counterparty_account_id BIGINT NULL REFERENCES accounts(account_id),

    -- ---- issue #004: khoá chống trừ tiền hai lần ---------------------------
    -- Sinh ở phía CLIENT, không phải phía server: đó mới là ý nghĩa của
    -- idempotency. Khách bấm "Thanh toán", mạng chậm, client retry -> lần thứ
    -- hai mang ĐÚNG khoá cũ -> UNIQUE từ chối -> không trừ tiền lần nữa.
    -- Ràng buộc nằm ở DB chứ không ở tầng ứng dụng, vì đây là hàng rào cuối
    -- cùng: mọi writer đều phải đi qua nó, kể cả script chạy tay.
    -- DEFAULT chỉ để tiện cho INSERT thủ công; faker LUÔN truyền khoá tường minh.
    idempotency_key UUID NOT NULL DEFAULT gen_random_uuid(),

    amount      DECIMAL(15,2) NOT NULL CHECK (amount > 0),  -- độ chính xác tiền tệ
    currency    CHAR(3)      NOT NULL DEFAULT 'USD',        -- ISO 4217; DEFAULT trung tính
    txn_type    txn_type     NOT NULL,
    status      txn_status   NOT NULL DEFAULT 'PENDING',
    channel     txn_channel  NOT NULL,
    -- ---- Gian lận: HAI cột, cố ý tách bạch -------------------------------
    -- is_flagged  = hệ thống NGHI NGỜ (có cả báo nhầm lẫn bỏ sót)
    -- fraud_label = SỰ THẬT (đáp án, chỉ có vì đây là dữ liệu mô phỏng)
    -- Tách ra mới đo được độ chính xác cảnh báo. Gộp một cột thì cảnh báo luôn
    -- đúng 100% — vô nghĩa.
    -- ⚠️ Ở tỷ lệ gian lận THẬT (~0,1%) thì không ai nhìn ra, kể cả người lẫn máy
    -- (dự án fake-banking-data tự thừa nhận điều này). Nên gian lận ở đây được cấy
    -- theo CẤU TRÚC rõ ràng — một chuỗi giao dịch nhỏ dồn trong ít phút — chứ không
    -- phải bằng cách dịch nhẹ phân phối.
    is_flagged  BOOLEAN      NOT NULL DEFAULT false,
    fraud_label BOOLEAN      NOT NULL DEFAULT false,

    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),

    CONSTRAINT uq_transactions_idempotency UNIQUE (idempotency_key),

    -- TRANSFER phải có đích, và các loại khác thì không được có. Ràng buộc HAI
    -- CHIỀU chứ không chỉ "TRANSFER thì bắt buộc": nếu để một giao dịch POS vẫn
    -- kèm counterparty thì downstream không biết nên cộng tiền cho ai.
    CONSTRAINT ck_transfer_has_counterparty CHECK (
        (txn_type =  'TRANSFER' AND counterparty_account_id IS NOT NULL)
     OR (txn_type <> 'TRANSFER' AND counterparty_account_id IS NULL)
    ),
    -- Tự chuyển cho chính mình là vô nghĩa và làm hỏng phép đối soát (cùng một
    -- tài khoản vừa bị trừ vừa được cộng).
    CONSTRAINT ck_counterparty_not_self CHECK (
        counterparty_account_id IS NULL OR counterparty_account_id <> account_id
    ),
    -- issue #007 — merchant CHỈ dành cho giao dịch mua bán. Cùng khuôn mẫu HAI CHIỀU
    -- với ck_transfer_has_counterparty, chỉ ngược hướng: ở đó TRANSFER *phải có* đích,
    -- ở đây TRANSFER *không được có* merchant.
    -- Viết một chiều ("mua bán thì phải có merchant") là để ngỏ đúng cái lỗ đã sinh ra
    -- issue này: chuyển khoản và lương vẫn đeo được merchant mà không ai chặn.
    CONSTRAINT ck_merchant_only_for_purchases CHECK (
        (txn_type IN ('TRANSFER', 'SALARY') AND merchant_id IS NULL)
     OR (txn_type IN ('CREDIT', 'DEBIT')    AND merchant_id IS NOT NULL)
    )
);

-- Index phục vụ faker cập nhật trạng thái nhanh (tìm PENDING/COMPLETED để chuyển tiếp).
CREATE INDEX idx_txn_status ON transactions(status);
-- Backfill lịch sử 90 ngày + job purge đều lọc theo created_at; không có index này
-- thì mỗi vòng quét là một seq scan trên vài triệu dòng.
CREATE INDEX idx_txn_created_at ON transactions(created_at);

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
