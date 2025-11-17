# [SQL] Website Performance Analysis

<img width="1024" height="683" alt="image" src="https://github.com/user-attachments/assets/1b69d3f0-7fe9-492b-be50-b5f16bc5a149" />

# I. Introduction
In this project, I utilized advanced SQL techniques—such as sliding window functions and aggregation queries—within Google BigQuery to analyze e-commerce data. The analysis covered product performance, sales trends, discount effectiveness, customer retention, and inventory management. These insights enabled the Marketing and Sales teams to make informed, data-driven decisions that improved overall business outcomes.

# II. Dataset Access
The e-commerce dataset is hosted in a public Google BigQuery dataset. To access it, follow the steps below:

1. Log in to your Google Cloud Platform account and create a new project.  
2. Open the BigQuery console and select the newly created project.  
3. In the navigation panel, choose **“Add Data”** → **“Search a project”**.  
4. Enter the project ID: **`bigquery-public-data.google_analytics_sample.ga_sessions`** and press **Enter**.  
5. Select the **`ga_sessions_`** table to explore the dataset.

# III. Key Focus Areas
- **Product Performance Analysis:** Assessed product subcategory performance using sales metrics and year-over-year growth.  
- **Geographic Sales Patterns:** Identified top-performing regions based on order quantity across multiple years.  
- **Discount Strategy Assessment:** Analyzed seasonal discount spending across various product subcategories.  
- **Customer Retention Analysis:** Computed retention rates for successfully shipped orders through cohort analysis.  
- **Inventory Management:** Investigated stock level patterns, month-over-month changes, and stock-to-sales ratios.  
- **Order Status Monitoring:** Measured pending orders and their total value to evaluate fulfillment efficiency.

## IV. Exploring the Dataset
### Query 1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 
```sql
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
       SUM(totals.visits) AS visits, 
       SUM(totals.pageviews) AS pageviews,   
       SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;
```

| month  | visits | pageviews | transactions |
|--------|--------|-----------|--------------|
| 201701 | 64694  | 257708    | 713          |
| 201702 | 62192  | 233373    | 733          |
| 201703 | 69931  | 259522    | 993          |

Q1 2017 demonstrates steady website traffic, with March showing a significant surge in transactions (993). This increase suggests either an improvement in conversion rates or the influence of seasonal factors.

### Query 2: Bounce rate per traffic source in July 2017
```sql
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
```

| Row | source                | total_visits | total_no_of_bounces | bounce_rate |
|-----|-----------------------|--------------|---------------------|-------------|
| 1   | google                | 38400        | 19798               | 51.557      |
| 2   | (direct)              | 19891        | 8606                | 43.266      |
| 3   | youtube.com           | 6351         | 4238                | 66.730      |
| 4   | analytics.google.com  | 1972         | 1064                | 53.955      |
| 5   | Partners              | 1788         | 936                 | 52.349      |
| 6   | m.facebook.com        | 669          | 430                 | 64.275      |
| 7   | google.com            | 368          | 183                 | 49.728      |
| 8   | dfa                   | 302          | 124                 | 41.060      |
| 9   | sites.google.com      | 230          | 97                  | 42.174      |
| 10  | facebook.com          | 191          | 102                 | 53.403      |

Google contributes the largest share of traffic but also records a high bounce rate. YouTube and Facebook exhibit the highest bounce rates overall, whereas direct traffic shows stronger user engagement.

### Query 3: Revenue by traffic source by week, by month in June 2017
```sql
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

```

| Row | time_type | time   | source   | revenue    |
|-----|-----------|--------|----------|------------|
| 1   | Month     | 201706 | (direct) | 97333.6197 |
| 2   | Week      | 201724 | (direct) | 30908.9099 |
| 3   | Week      | 201725 | (direct) | 27295.3199 |
| 4   | Month     | 201706 | google   | 18757.1799 |
| 5   | Week      | 201723 | (direct) | 17325.6799 |
| 6   | Week      | 201726 | (direct) | 14914.8100 |
| 7   | Week      | 201724 | google   | 9217.1700  |
| 8   | Month     | 201706 | dfa      | 8862.2300  |
| 9   | Week      | 201722 | (direct) | 6888.9000  |
| 10  | Week      | 201726 | google   | 5330.5700  |


Direct traffic consistently delivers the highest revenue across both weekly and monthly periods. Google ranks as the second-largest revenue source. June stands out with strong overall performance, particularly driven by direct traffic.

### Query 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
```sql
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
```

| Row | month  | avg_pageviews_purchase  | avg_pageviews_non_purchase  |
|-----|--------|-------------------------|-----------------------------|
| 1   | 201706 | 94.02050113895217        | 316.86558846341671           |
| 2   | 201707 | 124.23755186721992       | 334.05655979568053           |

Non-purchasers show a noticeably higher average number of pageviews compared to purchasers. Both groups experienced an increase in pageviews from June to July, with purchasers demonstrating a larger relative growth.

### Query 5: Average number of transactions per user that made a purchase in July 2017
```sql
SELECT  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d', date)) AS Month,
        SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
UNNEST (hits) AS hits,
UNNEST (hits.product) AS product
WHERE totals.transactions >=1
      AND product.productRevenue is not null
GROUP BY Month
ORDER BY Month;
```

| Month  | Avg_total_transactions_per_user |
|--------|---------------------------------|
| 201707 | 4.16390041493776                |


In July 2017, users who made purchases completed an average of 4.16 transactions. This indicates a moderate level of repeat buying behavior among customers during the month.

### Query 6: Average amount of money spent per session. Only include purchaser data in July 2017
```sql
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
  and totals.transactions>=1
group by month;
```

| Month  | avg_revenue_by_user_per_visit |
|--------|-------------------------------|
| 201707 | 43.86                         |


In July 2017, purchasing users spent an average of **$43.86 per session**.  
This metric reflects the typical transaction value and provides useful insight into customer spending behavior, supporting more effective pricing and promotional strategies.

### Query 7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017
```sql
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
```

| Row | other_purchased_products                | quantity |
|-----|-----------------------------------------|----------|
| 1   | Google Sunglasses                       | 20       |
| 2   | Google Women's Vintage Hero ...         | 7        |
| 3   | SPF-15 Slim & Slender Lip Balm          | 6        |
| 4   | Google Women's Short Sleeve ...         | 4        |
| 5   | YouTube Men's Fleece Hoodie ...         | 3        |
| 6   | Google Men's Short Sleeve Bad...        | 3        |
| 7   | Android Men's Vintage Henley            | 2        |
| 8   | 22 oz YouTube Bottle Infuser            | 2        |
| 9   | Google Men's Short Sleeve Her...        | 2        |
| 10  | Android Women's Fleece Hoodie           | 2        |

Customers who purchased the **YouTube Men's Vintage Henley** also showed a preference for Google-branded products, particularly **Sunglasses**.  
Overall, there is a strong inclination toward casual wear and accessories across the **Google**, **YouTube**, and **Android** product lines.

### Query 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017
```sql
with
product_view as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '2'
  GROUP BY 1
),

add_to_cart as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_addtocart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '3'
  GROUP BY 1
),

purchase as(
  SELECT
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
    count(product.productSKU) as num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  , UNNEST(hits) AS hits
  , UNNEST(hits.product) as product
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
  AND hits.eCommerceAction.action_type = '6'
  and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
  group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;
```

| Row | month  | num_product_view | num_addtocart | num_purchase | add_to_cart_rate | purchase_rate |
|-----|--------|------------------|---------------|--------------|------------------|---------------|
| 1   | 201701 | 25787            | 7342          | 2143         | 28.47            | 8.31          |
| 2   | 201702 | 21489            | 7360          | 2060         | 34.25            | 9.59          |
| 3   | 201703 | 23549            | 8782          | 2977         | 37.29            | 12.64         |


Product view-to-purchase conversion rates steadily improved from January to March 2017.  
March recorded the highest engagement, with a **37.29% add-to-cart rate** and a **12.64% purchase rate**.  
This trend suggests growing effectiveness in converting browsers into buyers, likely driven by enhanced marketing efforts or improved user experience.

## V. Conclusion
Overall, this project leveraged **Google BigQuery** to analyze an e-commerce dataset, uncovering valuable insights related to product performance, customer behavior, and sales trends.  

Key findings include:  
- Improving conversion rates  
- Identifying cross-selling opportunities among branded items  
- Highlighting geographic sales patterns  

The analysis also pinpointed areas for optimization in marketing strategies, inventory management, and customer experience.  

By utilizing big data analytics, the project establishes a foundation for **data-driven decision-making**, enabling the business to:  
- Enhance product offerings  
- Refine pricing strategies  
- Boost customer retention  

These insights equip the company to make informed strategic decisions, supporting improved performance and growth in the competitive e-commerce landscape.

