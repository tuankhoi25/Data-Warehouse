-- Dim/Fact Layer

CREATE TABLE gold.dim_customer ON CLUSTER 'cluster_2S_2R' (
    customer_key UInt64,
    customer_id UInt64,
    name String,
    sex String,
    mail String,
    birthdate DateTime64(3),
    signup_date DateTime64(3)
)
ENGINE = MergeTree ()
ORDER BY (customer_key)
PRIMARY KEY (customer_key);

CREATE TABLE gold.dim_date ON CLUSTER 'cluster_2S_2R' (
    date_key UInt64,
    full_date DateTime64(3),
    day UInt8,
    month UInt8,
    month_name String,
    quarter UInt8,
    quarter_name String,
    year UInt8,
    day_of_week UInt8,
    day_name String,
    week_of_year UInt8,
    is_weekend Bool,
    is_holiday Bool
)
ENGINE = MergeTree ()
ORDER BY (date_key)
PRIMARY KEY (date_key);


CREATE TABLE gold.dim_location ON CLUSTER 'cluster_2S_2R' (
    location_key UInt64,
    location_id String,
    street_address String,
    city String,
    state String,
    country String,
    zipcode String
)
ENGINE = MergeTree ()
ORDER BY (location_key)
PRIMARY KEY (location_key);

CREATE TABLE gold.bridge_customer_location ON CLUSTER 'cluster_2S_2R' (
    customer_location_key UInt64,
    customer_location_id UInt64,
    location_key UInt64,
    customer_key UInt64,
    valid_from DateTime64(3),
    valid_to DateTime64(3),
    is_current Bool
)
ENGINE = MergeTree ()
ORDER BY (customer_location_key)
PRIMARY KEY (customer_location_key);

CREATE TABLE gold.dim_product ON CLUSTER 'cluster_2S_2R' (
    product_key UInt64,
    product_id String,
    product_title String,
    currency String,
    price Decimal64(2),
    valid_from DateTime64(3),
    valid_to DateTime64(3),
    is_current Bool
)
ENGINE = MergeTree ()
ORDER BY (product_key)
PRIMARY KEY (product_key);

CREATE TABLE gold.dim_category ON CLUSTER 'cluster_2S_2R' (
    category_key UInt64,
    category_id UInt64,
    category_name String
)
ENGINE = MergeTree ()
ORDER BY (category_key)
PRIMARY KEY (category_key);

CREATE TABLE gold.bridge_product_category ON CLUSTER 'cluster_2S_2R' (
    product_category_key UInt64,
    product_category_id UInt64,
    product_key UInt64,
    category_key UInt64
)
ENGINE = MergeTree ()
ORDER BY (product_category_key)
PRIMARY KEY (product_category_key);

CREATE TABLE gold.fact_review ON CLUSTER 'cluster_2S_2R' (
    review_key UInt64,
    review_id String,
    product_key UInt64,
    customer_key UInt64,
    date_key UInt64,
    star_rating UInt8,
    helpful_votes UInt32,
    total_votes UInt32,
    marketplace String,
    verified_purchase Bool,
    review_headline String,
    review_body String,
    is_current Bool
)
ENGINE = MergeTree ()
ORDER BY (review_key)
PRIMARY KEY (review_key);