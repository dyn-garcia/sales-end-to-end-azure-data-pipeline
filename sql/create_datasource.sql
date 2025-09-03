-- create master key add your own password -- 
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '' 

-- create scoped credential managed identity -- 
CREATE DATABASE SCOPED CREDENTIAL cred_sales
WITH IDENTITY = 'Managed Identity' 

-- create external silver data source 
CREATE EXTERNAL DATA SOURCE source_silver_sales 
WITH
( 
    LOCATION = 'https://storagesalesprodcontoso.dfs.core.windows.net/silver',
    CREDENTIAL = cred_sales 
    ) 
