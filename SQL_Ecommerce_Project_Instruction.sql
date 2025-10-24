--Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
  --COUNT(DISTINCT(fullVisitorId)) AS visits,
  COUNT(fullVisitorId) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE 
  _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month

--Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT
  trafficSource.source AS source,
  COUNT(fullVisitorId) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  ROUND((SUM(totals.bounces) / COUNT(*))*100, 3) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE 
  _TABLE_SUFFIX BETWEEN '0701' AND '0731'
GROUP BY source
ORDER BY total_visits DESC

--Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.
WITH PurchaserData AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(totals.pageviews) AS total_pageviews,
    COUNT(DISTINCT fullVisitorId) AS unique_users
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '0601' AND '0731'
    AND totals.transactions >= 1
    AND product.productRevenue IS NOT NULL
  GROUP BY month
),
NonPurchaserData AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(totals.pageviews) AS total_pageviews,
    COUNT(DISTINCT fullVisitorId) AS unique_users
  FROM
   `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '0601' AND '0731'
    AND totals.transactions IS NULL
    AND product.productRevenue IS NULL
  GROUP BY month
)
SELECT
  p.month,
  ROUND((p.total_pageviews)/(p.unique_users), 8) AS avg_pageviews_purchase,
  ROUND((np.total_pageviews)/(np.unique_users), 8) AS avg_pageviews_non_purchase
FROM PurchaserData p
JOIN NonPurchaserData np
  ON p.month = np.month
ORDER BY p.month;

--Query 05: Average number of transactions per user that made a purchase in July 2017
WITH purchasers AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    fullVisitorId,
    SUM(totals.transactions) AS total_transactions
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hit,
    UNNEST(hit.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '0701' AND '0731'
    AND totals.transactions >= 1
    AND product.productRevenue IS NOT NULL
  GROUP BY
    fullVisitorId,
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) 
)

SELECT
  month,
  AVG(total_transactions) AS Avg_total_transactions_per_user
FROM
  purchasers
GROUP BY
  month

--Query 06: Average amount of money spent per session. Only include purchaser data in July 2017
WITH PurchaserRevenue AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS Month,
    SUM(product.productRevenue / 1000000) AS total_revenue,
    COUNT(*) AS total_visits
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '0701' AND '0731'
    AND totals.transactions IS NOT NULL
    AND product.productRevenue IS NOT NULL
  GROUP BY Month
)
SELECT
  Month,
  ROUND(SAFE_DIVIDE(total_revenue, total_visits), 2) AS avg_revenue_by_user_per_visit
FROM PurchaserRevenue;
--Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
WITH target_customers AS (
  SELECT DISTINCT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE 
    _TABLE_SUFFIX BETWEEN '0701' AND '0731'
    AND totals.transactions >= 1
    AND product.productRevenue IS NOT NULL
    AND product.v2ProductName = "YouTube Men's Vintage Henley"
)

SELECT
  product.v2ProductName AS other_product,
  SUM(product.productQuantity) AS quantity_ordered
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` AS s,
  UNNEST(s.hits) AS hits,
  UNNEST(hits.product) AS product
JOIN target_customers tc
  ON s.fullVisitorId = tc.fullVisitorId
WHERE
  _TABLE_SUFFIX BETWEEN '0701' AND '0731'
  AND totals.transactions >= 1
  AND product.productRevenue IS NOT NULL
  AND product.v2ProductName != "YouTube Men's Vintage Henley"
GROUP BY other_product
ORDER BY quantity_ordered DESC

 	
--Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.
--Add_to_cart_rate = number product  add to cart/number product view. Purchase_rate = number product purchase/number product view. The output should be calculated in product level.
WITH database AS (
  SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    COUNTIF(hits.eCommerceAction.action_type = '2') AS num_product_view,
    COUNTIF(hits.eCommerceAction.action_type = '3') AS num_addtocart,
    COUNTIF(hits.eCommerceAction.action_type = '6' AND product.productRevenue IS NOT NULL) AS num_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '0101' AND '0331'
  GROUP BY month
)
SELECT
  month,
  num_product_view,
  num_addtocart,
  num_purchase,
  ROUND(SAFE_DIVIDE(num_addtocart, num_product_view) * 100, 2) AS add_to_cart_rate,
  ROUND(SAFE_DIVIDE(num_purchase, num_product_view) * 100, 2) AS purchase_rate
FROM database
ORDER BY month;

------------------------- KẾT QUẢ KHÁC ĐÁP ÁN ------------------------------------
--Query 3: Revenue by traffic source by week, by month in June 2017
WITH product_revenue_data AS (
  SELECT
    date,
    trafficSource.source AS source,
    product.productRevenue / 1000000 AS revenue 
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hit,
    UNNEST(hit.product) AS product
  WHERE 
    _TABLE_SUFFIX BETWEEN '0601' AND '0630'
    AND product.productRevenue IS NOT NULL 
)

-- revenue of moth
SELECT 
  'Month' AS time_type,
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS time,
  source,
  ROUND(SUM(revenue), 4) AS revenue
FROM product_revenue_data
GROUP BY time, source
UNION ALL
-- revenue of week
SELECT 
  'Week' AS time_type,
  FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time,
  source,
  ROUND(SUM(revenue), 4) AS revenue
FROM product_revenue_data
GROUP BY time, source
ORDER BY source,time


