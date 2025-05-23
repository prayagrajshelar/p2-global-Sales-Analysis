# p2-global-Sales-Analysis
This project is designed to build an end-to-end **ETL pipeline on Google Cloud Platform (GCP)** to analyze global sales data across multiple countries. The objective is to clean, transform, unify, and analyze sales data from various formats and sources to generate business insights, visualizations, and forecasts that assist stakeholders in decision-making.
End-to-end ETL pipeline on GCP to collect, clean, transform, and analyze daily sales data from 8 countries across 5 product categories. Includes unified INR conversion, tax reports, dashboards, and monthly sales forecasting for business insights.

## 🧰 Tools & Services Used

- **GCP Services**:  
  - Cloud Storage  
  - Cloud SQL (MySQL, SQL Server, PostgreSQL in AlloyDB)  
  - BigQuery  
  - Cloud Composer (Airflow)  
  - Data Studio or Looker Studio

- **Languages/Frameworks**:  
  - Python (ETL Logic)  
  - SQL  
  - Airflow for orchestration  
  - Pandas for transformation  


Stpes:
# 1. Data Gathering
In Data gathering Stage, we  will gather sales data from different countries. data might in different formats like CSV, JSON, SQL file, XLSX.

# 2. Data Ingestion
In Data ingestion stage, we ingest all gathered data into particular locations.
for eg.
1. Use GCS bucket to store csv and xlsx files.
2. Use another bucket for Json file.
3. Use different cloud sql instances for different countries.

## 🗂️ Data Sources

| Country      | Source Type         | Format       | Notes                                |
|--------------|---------------------|--------------|--------------------------------------|
| India        | SQL Server          | RDBMS        | Structured                           |
| Japan        | Cloud Storage       | CSV          | Semi-structured                      |
| Norway       | AlloyDB             | PostgreSQL   | Structured                           |
| Sri Lanka    | Cloud Storage       | JSON         | Semi-structured (different bucket)   |
| Hong Kong    | Local Storage       | Excel        | Unstructured                         |
| Oman         | MySQL               | RDBMS        | Structured (own DB)                  |
| Germany      | MySQL               | RDBMS        | Structured (own DB)                  |
| Qatar        | MySQL               | RDBMS        | Structured (own DB)                  |


# 3. ETl pipeline
1. **Extract**: Load data from SQL/CSV/JSON/Excel sources.
2. **Transform**:
   - Convert currencies to INR.
   - Handle null values.
   - Normalize schema to standard format:  
     `SaleId, Country, Category, Product, Qty, Amount`
3. **Load**: Store final cleaned and unified data in **BigQuery**.


## 📊 Data Analysis & Reporting

### 📁 A. Sales Summary Report
- Deliverable: Google Cloud link (CSV/Excel)
- Fields: `SaleId`	`Country`	`Category`	`Product`	`Qty`	`Price`	`Amount`	`INR_Amount`

### 📈 B. Graphical Dashboards
- Visualizations by:
  - Product Category
  - Country
  - Overall sales trend

### 💰 C. Tax Department Views
- Report with:
  - Sales (Product/Country/Category-wise)
  - Tax Value (5% on each sale)
  - Tables and visuals


## 📤 Deliverables
- Cleaned consolidated dataset in INR.
- Dashboard link (e.g., Looker Studio).
- Forecast reports.

## 📃 License
© 2025 Global Sales Analytics | All Rights Reserved.

