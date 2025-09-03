-- SALES
CREATE OR ALTER VIEW gold.sales AS
SELECT *
FROM OPENROWSET(
        BULK 'sales/',                    -- abfss://silver/.../sales/ (Delta root)
        DATA_SOURCE = 'source_silver_sales',
        FORMAT = 'DELTA'
     ) AS rows;
GO
-- STORE
CREATE OR ALTER VIEW gold.store AS
SELECT *
FROM OPENROWSET(
        BULK 'store/',
        DATA_SOURCE = 'source_silver_sales',
        FORMAT = 'DELTA'
     ) AS rows;
GO
-- PRODUCT
CREATE OR ALTER VIEW gold.product AS
SELECT *
FROM OPENROWSET(
        BULK 'product/',
        DATA_SOURCE = 'source_silver_sales',
        FORMAT = 'DELTA'
     ) AS rows;
GO
-- CUSTOMER
CREATE OR ALTER VIEW gold.customer AS
SELECT *
FROM OPENROWSET(
        BULK 'customer/',
        DATA_SOURCE = 'source_silver_sales',
        FORMAT = 'DELTA'
     ) AS rows;