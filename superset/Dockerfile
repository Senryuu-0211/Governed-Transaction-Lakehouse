# Superset 4.1.1 không bundle driver Postgres. Ta dùng Postgres cho CẢ metadata
# lẫn marts nên cần psycopg2. Thêm đúng 1 package, giữ image gọn.
FROM apache/superset:4.1.1
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
USER superset
