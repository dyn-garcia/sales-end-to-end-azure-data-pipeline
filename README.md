# sales-end-to-end-azure-data-pipeline
End-to-end Data Engineering Project | PostgreSQL → ADLS → Databricks → Synapse → Power BI

This project demonstrates a **modern data engineering pipeline on Azure**, transforming sales data from **raw ingestion to gold, analytics-ready views**, enabling business insights in **Power BI**.  

---

## 📊 Project Overview
- **Source System:** PostgreSQL database (Sales data)  
- **Destination:** Power BI dashboards for visualization  
- **Cloud Platform:** Microsoft Azure  
- **Data Flow:** Raw ➝ Bronze ➝ Silver ➝ Gold  

The pipeline simulates a real-world **ETL workflow**, where raw transactional data is ingested, cleaned, transformed, and enriched into business-ready insights.  

---

## 🏗️ Architecture Diagram
<img width="2570" height="565" alt="Untitled Diagram drawio (2)" src="https://github.com/user-attachments/assets/315eee07-0f03-4606-a438-587301587331" />

---

## ⚙️ Tech Stack
- **PostgreSQL** → Source system (sales data)
- **Azure Data Factory (ADF)** → Data ingestion
- **Azure Data Lake Gen2** → Raw / Bronze / Silver / Gold layers
- **Azure Databricks (PySpark, Delta Lake)** → Data processing & transformations
- **Azure Synapse Analytics (SQL)** → Serving analytics-ready views
- **Power BI** → Data visualization
  
---

## 🔄 Pipeline Steps

**1. Ingestion (Raw Layer)**  
I built ADF pipelines that connect to PostgreSQL and copy the data into Azure Data Lake Storage.  
Instead of hardcoding tables, I parameterized the pipeline so it can ingest any table dynamically.  
This makes it easy to scale, if tomorrow a new table is added in the database, I don’t need to build a new pipeline.  
<img width="882" height="693" alt="image" src="https://github.com/user-attachments/assets/f5bc5ae3-41bb-44c6-a938-47426c62de53" />

**2. Bronze Layer — Upsert Snapshot via MERGE + CDF ON** 
The daily snapshots from Postgres are ingested into Delta tables with Change Data Feed (CDF) enabled.  
Rather than just appending files, I used a MERGE strategy with a `row_hash` so Delta can track real inserts, updates, and deletes.  
This ensures the Bronze layer faithfully reflects the source while still keeping the benefits of time travel and schema enforcement.  

**3. Silver Layer – Incremental Data Processing**  
From Bronze, I read only the new changes using CDF and a watermark, so the pipeline doesn’t waste time reprocessing old data.  
Before loading into Silver, I applied transformations like currency conversion, standardizing dimension tables, and building enriched columns such as full names and addresses.  
Then I MERGE the changes into Silver — inserting new rows, updating only when data has actually changed, and applying deletes when records are removed at the source.  
 
**4. Gold Layer — Analytics-Ready Views (Synapse)**  
On top of Silver, I created SQL scripts and views in Synapse that are directly usable for analytics.  
These views answer business questions such as yearly sales trends, top products by category, monthly revenue trends, and annual revenue by country store location/online.  
This layer acts as the trusted “single source of truth” for reporting.  

**5. Visualization — Power BI Sales Dashboard**  
Finally, I connected the Gold views to Power BI and built a clean sales dashboard.  
It includes a year slicer for filtering, KPI cards for revenue and growth, and charts for trends and product performance.  
This brings the data full circle — from raw source tables all the way to business insights for decision makers.  
![Power BI Sales Dashboard](https://github.com/user-attachments/assets/e204c556-0168-4005-9f42-c318a929d1e3)
