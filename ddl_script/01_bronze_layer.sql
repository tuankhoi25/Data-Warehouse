-- Lưu data lịch sử

CREATE TABLE bronze.customer ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    name String,
    sex String,
    mail String,
    birthdate DateTime64(3),
    login_username String,
    login_password String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.location ON CLUSTER 'cluster_2S_2R' (
    id String,
    street_address String,
    city String,
    state String,
    zipcode UInt8,
    country String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.customer_location ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id UInt64,
    location_id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.phone_number ON CLUSTER 'cluster_2S_2R' (
    id String,
    phone_number String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.customer_phone ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    customer_id UInt64,
    phone_id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.shadow_product ON CLUSTER 'cluster_2S_2R' (
    id String,
    product_id String,
    product_title String,
    currency String,
    price Decimal64(2),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    category_name String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.product_category ON CLUSTER 'cluster_2S_2R' (
    id UInt64,
    product_id String,
    category_id UInt64,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);

CREATE TABLE bronze.review ON CLUSTER 'cluster_2S_2R' (
    id String,
    customer_id UInt64,
    product_id String,
    star_rating String,
    helpful_votes UInt32,
    total_votes UInt32,
    marketplace String,
    verified_purchase String,
    review_headline String,
    review_body String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _ingested_at DateTime64(3),
    _batch_id String,
    _is_deleted Bool
)
ENGINE = MergeTree ()
ORDER BY (updated_at, id)
PRIMARY KEY (updated_at, id);