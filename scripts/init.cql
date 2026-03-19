-- 建立管理員帳號 (admin/admin123)
CREATE USER IF NOT EXISTS admin WITH PASSWORD 'admin123' SUPERUSER;

-- 建立 Keyspace (若不存在)
CREATE KEYSPACE IF NOT EXISTS test_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- 建立 Users 資料表，改用複合分區鍵 (Time Bucket 設計)
CREATE TABLE IF NOT EXISTS test_keyspace.users (
    user_id text,
    time_bucket text,
    username text,
    email text,
    created_at timestamp,
    -- 複合分區鍵: (user_id, time_bucket) + 排序集群鍵: created_at
    PRIMARY KEY ((user_id, time_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);
