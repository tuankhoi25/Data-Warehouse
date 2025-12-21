-- Lưu current data sau khi transform

CREATE TABLE silver.customer ON CLUSTER 'cluster_2S_2R' (
    customer_id UInt64,
    name String,
    sex String,
    mail String,
    birthdate DateTime64(3),
    signup_date DateTime64(3)
)
ENGINE = MergeTree ()
ORDER BY (customer_id)
PRIMARY KEY (customer_id);

CREATE TABLE silver.location ON CLUSTER 'cluster_2S_2R' (
    location_id String,
    street_address String,
    city String,
    state String,
    country String,
    zipcode String
)
ENGINE = MergeTree ()
ORDER BY (location_id)
PRIMARY KEY (location_id);

CREATE TABLE silver.customer_location ON CLUSTER 'cluster_2S_2R' (
    customer_location_id UInt64,
    customer_id UInt64,
    location_id String,
    source_created_at DateTime64(3),
    source_updated_at DateTime64(3)
)
ENGINE = MergeTree ()
ORDER BY (customer_location_id)
PRIMARY KEY (customer_location_id);

CREATE TABLE silver.product ON CLUSTER 'cluster_2S_2R' (
    product_id String,
    product_title String,
    currency String,
    price Decimal64(2),
    source_created_at DateTime64(3),
    source_updated_at DateTime64(3)
)
ENGINE = MergeTree ()
ORDER BY (product_id)
PRIMARY KEY (product_id);

CREATE TABLE silver.category ON CLUSTER 'cluster_2S_2R' (
    category_id UInt64,
    category_name String
)
ENGINE = MergeTree ()
ORDER BY (category_id)
PRIMARY KEY (category_id);

CREATE TABLE silver.product_category ON CLUSTER 'cluster_2S_2R' (
    product_category_id UInt64,
    product_id String,
    category_id UInt64
)
ENGINE = MergeTree ()
ORDER BY (product_category_id)
PRIMARY KEY (product_category_id);

CREATE TABLE silver.review ON CLUSTER 'cluster_2S_2R' (
    review_id String,
    customer_id UInt64,
    product_id String,
    star_rating UInt8,
    helpful_votes UInt32,
    total_votes UInt32,
    marketplace String,
    verified_purchase Bool,
    review_headline String,
    review_body String,
    modified_date DateTime64(3)
)
ENGINE = MergeTree ()
ORDER BY (review_id)
PRIMARY KEY (review_id);