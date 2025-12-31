-- Đứa tất cả data type sang Nullable(T) để bắt cả null value, xem như source of truth
-- Chọn lại datatype cho sát với data source (ko chọn data type có precision) để tối ưu lưu trữ
-- Đảm nhiệm Schema Enforcement ở mức data type. Sau đó thì tới value enforcement ở _postgres__sources.yml

CREATE TABLE postgres.customer ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    name Nullable(String),
    sex Nullable(FixedString(1)),
    mail Nullable(String),
    birthdate Nullable(Date),
    login_username String,
    login_password String,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_customer', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.location ON CLUSTER 'cluster_2S_2R' (
    id UUID,
    street_address String,
    city Nullable(String),
    state Nullable(String),
    zipcode Int32,
    country String,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_location', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.customer_location ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id UInt64,
    location_id UUID,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_customer_location', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.phone_number ON CLUSTER 'cluster_2S_2R' (
    id UUID,
    phone_number String,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_phone_number', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.customer_phone ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id UInt64,
    phone_id UUID,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_customer_phone', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.shadow_product ON CLUSTER 'cluster_2S_2R' (
    id UUID,
    product_id String,
    product_title String,
    currency String,
    price Decimal(10, 2),
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_shadow_product', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    category_name String,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_category', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.product_category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    product_id String,
    category_id UInt64,
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_product_category', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.review ON CLUSTER 'cluster_2S_2R' (
    id String,
    customer_id UInt64,
    product_id String,
    star_rating Nullable(FixedString(1)),
    helpful_votes Nullable(Int32),
    total_votes Nullable(Int32),
    marketplace Nullable(FixedString(2)),
    verified_purchase Nullable(FixedString(1)),
    review_headline Nullable(String),
    review_body Nullable(String),
    created_at Date,
    updated_at Nullable(Date)
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'dev_review', 'clickhouse_user', 'clickhouse_password');