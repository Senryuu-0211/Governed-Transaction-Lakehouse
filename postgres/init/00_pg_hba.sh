#!/bin/bash
# =============================================================================
# Cho phép REPLICATION connection cho role debezium — chạy khi init (volume trống).
# =============================================================================
# Debezium (pgoutput) mở một REPLICATION connection để đọc logical decoding, KHÔNG
# phải connection thường. Trong pg_hba, database=`replication` là mục ĐẶC BIỆT — dòng
# `host all all all` KHÔNG bao gồm nó. Thiếu dòng này => connector fail với
# "no pg_hba.conf entry for replication connection". Đây là bẫy CDC kinh điển.
#
# Chỉ mở cho đúng role `debezium` (least-privilege), auth scram (khớp mật khẩu role).
set -e
echo "host replication debezium all scram-sha-256" >> "$PGDATA/pg_hba.conf"
echo ">> pg_hba: enabled replication for role 'debezium'"
