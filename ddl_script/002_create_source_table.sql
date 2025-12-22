-- 1. Bảng Customer
CREATE TABLE postgres.customer ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    name Nullable(String),
    sex Nullable(String),
    mail Nullable(String),
    birthdate Nullable(DateTime64(3)),
    login_username Nullable(String),
    login_password Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'customer', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.location ON CLUSTER 'cluster_2S_2R' (
    id String,
    street_address Nullable(String),
    city Nullable(String),
    state Nullable(String),
    zipcode Nullable(UInt32),
    country Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'location', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.customer_location ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id Nullable(UInt64),
    location_id Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'customer_location', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.phone_number ON CLUSTER 'cluster_2S_2R' (
    id String,
    phone_number Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'phone_number', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.customer_phone ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id Nullable(UInt64),
    phone_id Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'customer_phone', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.shadow_product ON CLUSTER 'cluster_2S_2R' (
    id String,
    product_id Nullable(String),
    product_title Nullable(String),
    currency Nullable(String),
    price Nullable(Decimal64(2)),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'shadow_product', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    category_name Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'category', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.product_category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    product_id Nullable(String),
    category_id Nullable(UInt64),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'product_category', 'clickhouse_user', 'clickhouse_password');

CREATE TABLE postgres.review ON CLUSTER 'cluster_2S_2R' (
    id String,
    customer_id Nullable(UInt64),
    product_id Nullable(String),
    star_rating Nullable(String),
    helpful_votes Nullable(UInt32),
    total_votes Nullable(UInt32),
    marketplace Nullable(String),
    verified_purchase Nullable(String),
    review_headline Nullable(String),
    review_body Nullable(String),
    created_at Nullable(DateTime64(3)),
    updated_at Nullable(DateTime64(3))
)
ENGINE = PostgreSQL('postgres:5432', 'oltp', 'review', 'clickhouse_user', 'clickhouse_password');