# 🧠 Layoffs Data Cleaning & Analysis

This project demonstrates a complete SQL data cleaning and exploratory analysis workflow using the `world_layoffs` dataset. It mimics a real-world business scenario where raw data requires thorough preparation before insights can be drawn.

---

## 🧩 Objective

- Clean raw layoff data
- Standardize inconsistent formats
- Handle missing and duplicate entries
- Perform deep exploratory analysis to identify trends in layoffs across companies, industries, countries, and time

---

## 🛠️ Tools & Technologies

- **SQL (MySQL)**: Core technology used for all transformations and analysis
- **MySQL Workbench** or any SQL IDE

---

## 🧪 Key SQL Skills Demonstrated

### ✅ Data Cleaning Techniques
- Creating **staging tables** to protect raw data
- Removing **duplicates** using `ROW_NUMBER()` window function
- Replacing blank strings with `NULL`
- Using `TRIM()` and `REPLACE()` for string cleanup
- Parsing dates with `STR_TO_DATE()` and altering data types

### ✅ Advanced SQL Features
- **Window Functions**:
  - `ROW_NUMBER()` for deduplication
  - `DENSE_RANK()` for ranking
  - `SUM() OVER()` for rolling totals

- **CTEs (Common Table Expressions)**:
  - For layered queries and reusable logic

- **Self-Joins**:
  - To fill missing values based on matching company names

### ✅ Exploratory Data Analysis
- Aggregations by company, industry, country, stage, and time
- Monthly and yearly trend analysis
- Top 5 companies by layoffs each year

---

## 📊 Sample Insights

- Industries most affected by layoffs
- Countries with the highest job cuts
- Companies with 100% workforce reduction
- Monthly layoff trends and rolling totals

---
