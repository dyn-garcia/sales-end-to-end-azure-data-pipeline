-- create yearly sales data-
DROP VIEW IF EXISTS gold.yearly_sales;
GO

CREATE VIEW gold.yearly_sales
AS
SELECT
    YEAR(orderdate)  AS [year],
    MONTH(orderdate) AS [month],
    FORMAT(orderdate, 'MMM') AS month_abbr,  
    DATEFROMPARTS(YEAR(orderdate), MONTH(orderdate), 1) AS year_month,
    SUM(quantity * netprice)              AS revenue,
    SUM(quantity * unitcost)              AS cogs,
    SUM((netprice - unitcost) * quantity) AS profit,
    (SUM((netprice - unitcost) * quantity)
        / NULLIF(SUM(quantity * netprice), 0)) * 100.0 AS margin_pct
FROM gold.sales
GROUP BY YEAR(orderdate), MONTH(orderdate), FORMAT(orderdate, 'MMM');
GO

-- top selling product per category per year --
CREATE OR ALTER VIEW gold.top_selling_product_per_category
AS
WITH product_revenue AS (
    SELECT
        b.categoryname AS productcategory,
        b.productname  AS productname,
        YEAR(a.orderdate) AS sales_year,
        SUM(a.netprice * a.quantity) AS total_revenue
    FROM (
        SELECT DISTINCT orderdate, productkey, netprice, quantity
        FROM gold.sales
    ) AS a
    LEFT JOIN gold.product AS b 
        ON a.productkey = b.productkey
    GROUP BY 
        b.categoryname, 
        b.productname,
        YEAR(a.orderdate)
),
ranked_product AS (
    SELECT
        productcategory,
        productname,
        total_revenue,
        sales_year,
        RANK() OVER (
            PARTITION BY productcategory, sales_year
            ORDER BY total_revenue DESC
        ) AS rnk
    FROM product_revenue
)
SELECT
    productcategory,
    productname,
    total_revenue,
    sales_year,
    rnk
FROM ranked_product
WHERE rnk = 1;


-- store country location revenue per year --
CREATE OR ALTER VIEW gold.store_country_revenue_per_year
AS
SELECT
    b.countryname AS countryname,
    YEAR(a.orderdate) AS sales_year,
    SUM(a.netprice * a.quantity) AS total_revenue
FROM (
    SELECT DISTINCT orderdate, storekey, netprice, quantity
    FROM gold.sales
) AS a
LEFT JOIN gold.store AS b 
    ON a.storekey = b.storekey
GROUP BY 
    b.countryname, 
    YEAR(a.orderdate);



SELECT * FROM gold.product
 

