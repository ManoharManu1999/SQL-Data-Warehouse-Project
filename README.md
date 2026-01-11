# Data Warehouse and Analytics Project

This repository contains an end-to-end **SQL Server data warehouse project** built to demonstrate practical skills in data engineering, ETL development, and analytical data modeling.

The project focuses on transforming raw operational data from multiple source systems into a clean, structured, and analytics-ready data model that supports reporting and decision-making.

This is a **portfolio project**, designed to closely reflect real-world data warehouse implementations and best practices.

---

## Project Overview

In many organizations, data is distributed across multiple systems such as CRM and ERP platforms. This data is often inconsistent, duplicated, and not suitable for direct analysis.

In this project, I designed and implemented a **modern data warehouse** that:

- Ingests raw data from multiple source systems  
- Cleans and standardizes the data  
- Applies data quality and validation rules  
- Models the data using dimensional modeling  
- Exposes business-ready datasets for analytics and BI tools  

The entire solution is implemented using **SQL Server and T-SQL**, following industry-standard practices.

This repository is suitable for professionals and students looking to showcase or build skills related to:

- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## Data Architecture

The project follows the **Medallion Architecture** pattern with three layers: **Bronze**, **Silver**, and **Gold**.

![Data Architecture](docs/data_architecture.png)

```
Source Systems (CSV Files)
↓
Bronze Layer (Raw Data)
↓
Silver Layer (Cleaned & Conformed Data)
↓
Gold Layer (Business-Ready Views)
```

### Bronze Layer – Raw Data
- Stores data exactly as received from source systems  
- Data is loaded from CSV files using `BULK INSERT`  
- No transformations or business rules are applied  
- Acts as a raw landing zone for incoming data  

### Silver Layer – Cleaned Data
- Applies data cleansing, standardization, and validation rules  
- Removes duplicates and resolves data quality issues  
- Standardizes values such as gender, marital status, country codes, and dates  
- Produces consistent, analytics-ready datasets  

### Gold Layer – Analytics Layer
- Contains business-facing dimensions and fact tables  
- Modeled using a **star schema**  
- Implemented as views for ease of consumption  
- Designed for reporting and BI tools such as Power BI or Tableau  

---

## Data Modeling Approach

The Gold layer follows a **dimensional modeling** approach.
### Star Schema (Gold Layer)

The Gold layer is modeled using a star schema to support analytical queries
and reporting use cases.

![Data Model](docs/data_model.png)

### Dimensions
- **Customer Dimension**  
  Combines customer data from CRM and ERP systems into a single, consistent customer view.

- **Product Dimension**  
  Represents active products with category and hierarchy details.


### Fact Table
- **Sales Fact**  
  Stores transactional sales data at the order line level.

Each table has a clearly defined **grain**, ensuring correct analytical behavior and consistent query results.

---

## ETL Process

The ETL process is implemented using stored procedures and organized by layer:

- **Bronze Layer**
  - Full refresh loads using `TRUNCATE + BULK INSERT`
  - Focused on fast and reliable raw data ingestion  

- **Silver Layer**
  - Data transformation using T-SQL  
  - Window functions for deduplication  
  - Data quality rules and validation logic  

- **Gold Layer**
  - Business-ready views built on top of Silver tables  
  - No heavy transformations, only business alignment and joins  

Execution times are logged to track load duration for each step.

---

## Analytics & Reporting

The Gold layer enables analysis such as:
- Customer behavior analysis  
- Product performance evaluation  
- Sales trend analysis over time  

The data model is designed to support efficient SQL querying and easy integration with BI tools.

---

## Project Requirements

### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data and enable analytical reporting and informed decision-making.

### Scope & Specifications
- **Data Sources**: ERP and CRM systems provided as CSV files  
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis  
- **Integration**: Combine multiple sources into a unified analytical model  
- **Scope**: Focus on the latest dataset only (no historization required)  
- **Documentation**: Provide clear documentation for both technical and business users  

---

## Repository Structure

```
data-warehouse-project/
│
├── datasets/ # Source CSV files (CRM and ERP)
├── docs/ # Documentation and diagrams
├── scripts/ # SQL scripts (Bronze, Silver, Gold)
├── tests/ # Data quality and validation checks
├── README.md
├── LICENSE
└── requirements.txt
```

---

## Tools & Technologies Used

- SQL Server  
- T-SQL (DDL, DML, Stored Procedures)  
- SQL Server Management Studio (SSMS)  
- Git & GitHub for version control  
- Draw.io for architecture and data modeling diagrams  

All tools used in this project are freely available and widely used in industry.

---

## Key Learnings from This Project

- Designing a layered data warehouse using Bronze, Silver, and Gold concepts  
- Writing production-style SQL for ETL pipelines  
- Applying data quality and validation rules  
- Implementing dimensional models for analytics  
- Structuring SQL projects for clarity and maintainability  
- Documenting data models for both technical and non-technical audiences  

---

## How to Run the Project

1. Create the Bronze layer tables  
2. Execute the `bronze.load_bronze` stored procedure  
3. Create the Silver layer tables  
4. Execute the `silver.load_silver` stored procedure  
5. Create the Gold layer views  

---

## Documentation

Additional project documentation is available in the `docs/` folder:

- [Data Catalog](docs/data_catalog.md)
- [Naming Conventions](docs/naming_conventions.md)
- [ETL Concepts & Process](docs/ETL.png)
- [Detailed Layer Explanation (PDF)](docs/data_layers.pdf)


## About Me

I built this project to strengthen my understanding of **data warehousing, ETL design, and analytical modeling**, and to apply these concepts in a practical, hands-on way.

I enjoy working on data engineering problems that involve transforming raw data into meaningful insights, and I am actively interested in opportunities related to:

- Data Engineering  
- Analytics Engineering  
- SQL Development  
- Business Intelligence  

This project reflects how I approach data problems, with a strong focus on data quality, clarity, and usability.

---

## Stay in Touch


[![My Skills](https://skillicons.dev/icons?i=linkedin)](https://www.linkedin.com/in/manohark1999)
[![My Skills](https://skillicons.dev/icons?i=gmail)](mailto:manoharmanu.k1999@gmail.com)

 ---

## License

This project is licensed under the MIT License.  
You are free to use, modify, and share it with proper attribution.
