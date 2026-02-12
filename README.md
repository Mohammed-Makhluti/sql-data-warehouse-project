# Data Warehouse & ETL Project (PostgreSQL) 🚀

This project demonstrates the design and implementation of a comprehensive Data Warehouse solution using **PostgreSQL**. It is based on the methodology provided by **[Data With Baraa](https://www.youtube.com/@datawithbaraa)**, but has been fully migrated and optimized for a PostgreSQL environment.

---

## 🏗️ Data Architecture
The project follows the **Medallion Architecture**, ensuring data quality and structure as it moves through different stages:

[Image of Medallion Architecture Bronze Silver Gold layers]

1.  **Bronze Layer**: Raw data ingestion. Data is loaded "as-is" from CRM and ERP CSV files.
2.  **Silver Layer**: Data cleansing, standardization, and handling of nulls or duplicates.
3.  **Gold Layer**: The final analytical layer, structured in a **Star Schema** (Facts and Dimensions) for business reporting.

---

## 🚀 Technical Features
* **Automated ETL Pipeline**: Developed a robust **Stored Procedure** that automates the truncation and loading of 6 different source tables with a single command.
* **PostgreSQL Optimization**: Utilized the high-performance `COPY` command for rapid data ingestion.
* **Advanced Error Handling**: Implemented a `BEGIN...EXCEPTION` block (Try-Catch equivalent) to capture and log errors without crashing the pipeline.
* **Performance Monitoring**: The system automatically calculates and prints the duration of each load task using `clock_timestamp()` to monitor performance.

---

## 🛠️ Tech Stack & Tools
* **Database**: PostgreSQL
* **SQL Client**: pgAdmin / DataGrip 
* **Data Modeling**: Draw.io
* **Data Sources**: CRM and ERP datasets (CSV format)

---

## 📂 Project Structure
```text
├── datasets/           # Raw source files (CSV)
├── scripts/            # SQL scripts organized by layer
│   ├── bronze/         # DDL and Automated Loading Procedures
│   ├── silver/         # Data cleaning and transformation
│   └── gold/           # Dimensional modeling (Star Schema)
├── docs/               # Architecture diagrams and data models
└── README.md           # Project documentation
