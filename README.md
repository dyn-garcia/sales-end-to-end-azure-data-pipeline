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
```mermaid
flowchart LR
    A[PostgreSQL] -->|Ingest with ADF| B[ADLS Raw]
    B -->|Databricks Notebook| C[Delta Bronze]
    C -->|Transform & CDC| D[Delta Silver]
    D -->|Aggregate in Synapse| E[Gold Views]
    E -->|Consume| F[Power BI]
