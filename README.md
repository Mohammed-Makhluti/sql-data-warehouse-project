# SQL Data Warehouse Project: Medallion Architecture (PostgreSQL) 🚀

## 📌 Project Overview
This project is inspired by the **"Data with Baraa"** roadmap, originally designed for MS SQL Server. I have successfully **adapted, migrated, and implemented** the entire pipeline using **PostgreSQL**. 

The project demonstrates the end-to-end process of building a modern Data Warehouse using the **Medallion Architecture**, handling the technical nuances and syntax differences between SQL dialects (like adapting date functions and casting types) to transform raw data into high-value business insights.

## 🏗️ Architecture Layers
The project follows a structured data flow to ensure quality and scalability:

1. **Bronze Layer (Raw):** - Direct ingestion of raw CSV files into PostgreSQL.
   - Preserves the original data state for auditing.
2. **Silver Layer (Cleansed):**
   - Data cleaning, handling NULL values, and standardizing formats.
   - Migrated MS SQL logic to PostgreSQL (e.g., using `CASE` statements and casting for data integrity).
3. **Gold Layer (Business):**
   - Star Schema implementation (Fact and Dimension tables).
   - Advanced analytical views designed for executive reporting.

## 📊 Key Analytics & Business Logic
Using advanced PostgreSQL features, I implemented:

- **Customer 360 Analysis:** Segmenting customers into **VIP, Regular, and New** using precise `AGE()` and `EXTRACT` functions to calculate customer lifespan accurately.
- **Product Performance:** Classifying products as **High/Mid/Low Performers** based on revenue magnitude.
- **Inventory & Sales Metrics:** Calculating **Recency (months since last sale)**, **Average Order Value (AOV)**, and **Cumulative Revenue (Running Totals)**.
- **Advanced SQL Techniques:** Extensive use of **CTEs, Window Functions, and Aggregate Functions**.

## 🛠️ Tech Stack
- **Database:** PostgreSQL (Successfully migrated from MS SQL Server logic)
- **Language:** SQL (PostgreSQL Dialect)
- **Concepts:** ETL/ELT, Data Modeling, Star Schema, Data Cleansing, Customer Segmentation.

## 📁 Project Structure
```text
sql-data-warehouse-project/
├── scripts/
│   ├── bronze/       # Table DDL and Bulk Data Loading
│   ├── silver/       # Data Transformation and Cleaning
│   └── gold/         # Analytical Views and Final Fact/Dim Tables
└── docs/             # Database ERD and project documentation
