-- -- Overview (trên cả nền tảng)

-- -- Tổng số review đc thực hiện
-- SELECT 
--     COUNT(*) AS total_reivew 
-- FROM gold.fact_review;

-- -- Rating star trung bình
-- SELECT 
--     AVG(star_rating) / COUNT(*) AS avg_rating_star
-- FROM gold.fact_review;

-- -- Rating star trung bình trong khoảng thời gian
-- date_range=
-- SELECT
--     COUNT(star_rating) AS avg_rating_star,
--     date_key
-- FROM gold.fact_review
-- WHERE date_key IN date_range
-- GROUP BY date_key;

-- -- Số lượng reviews tăng lên mỗi ngày
-- -- trong khoảng thời gian
-- -- có thể chọn filterd_star_rating cụ thể
-- date_range=
-- filterd_star_rating=
-- SELECT
--     COUNT(star_rating) AS avg_rating_star,
-- FROM gold.fact_review
-- WHERE date_range IN date_range AND star_rating = filterd_star_rating
-- GROUP BY date_key;

-- -- Bar chart số lượng review theo star rating
-- -- trong khoảng thời gian
-- SELECT
--     star_rating,
--     COUNT(star_rating) AS count
-- FROM gold.fact_review
-- GROUP BY star_rating;

-- -- Line chart: Average rating theo thời gian
-- -- (ngày/tuần/tháng/năm)
-- date_range=
-- SELECT
--     AVG(star_rating) AS avg_rating_star,
-- FROM gold.fact_review
-- WHERE date_range IN date_range
-- GROUP BY date_key;

-- -- Horizontal bar chart: Top 10 sản phẩm có average
-- -- rating cao nhất trong khoảng thời gian chỉ định

-- SELECT
--     p.product_title
-- FROM gold.fact_review AS r
-- JOIN gold.dim_product AS p
-- ON r.product_key = p.product_key
-- GROUP BY r.product_key
-- ORDER BY AVG(r.star_rating) DESC
-- LIMIT 10;

-- -- Tỷ lệ reviews theo giới tính
-- SELECT
--     c.sex,
--     COUNT(c.sex) AS total_per_sex
-- FROM gold.fact_review AS r
-- JOIN gold.dim_customer AS c
-- ON r.customer_key = c.customer_key
-- GROUP BY c.sex;

-- -- Tỷ lệ reviews theo độ tuổi
-- SELECT
--     c.birthdate,
--     COUNT(c.birthdate) AS total_per_sex
-- FROM gold.fact_review AS r
-- JOIN gold.dim_customer AS c
-- ON r.customer_key = c.customer_key
-- GROUP BY c.birthdate;

-- -- 