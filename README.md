# p2-global-Sales-Analysis
End-to-end ETL pipeline on GCP to collect, clean, transform, and analyze daily sales data from 8 countries across 5 product categories. Includes unified INR conversion, tax reports, dashboards, and monthly sales forecasting for business insights.
Stpes:
# 1. Data Gathering
In Data gathering Stage, we  will gather sales data from different countries. data might in different formats like CSV, JSON, SQL file, XLSX.

# 2. Data Ingestion
In Data ingestion stage, we ingest all gathered data into particular locations.
for eg.
1. Use GCS bucket to store csv and xlsx files.
2. Use another bucket for Json file.
3. Use different cloud sql instances for different countries.
   eg. sql server instance is having indian sales data, likewise


# 3. ETl pipeline
