# Metadata-Driven File Validation Pipeline

## Project Overview

This project implements a metadata-driven file validation pipeline using Azure Data Lake Storage Gen2, Azure Databricks, PySpark, and Azure SQL Database.

The pipeline validates incoming CSV files before processing and automatically routes files to staging or rejected folders based on validation results.

---

## Architecture

Source Files
      |
      v
ADLS Landing
      |
      v
Azure Databricks
      |
      +-- Schema Validation
      +-- Duplicate Validation
      +-- Date Format Validation
      |
      v
+-------------+-------------+
|                           |
v                           v
Staging                 Rejected

---

## Technologies Used

- Azure Databricks
- PySpark
- Azure Data Lake Storage Gen2
- Azure SQL Database
- JDBC
- Azure Data Factory
- Azure Key Vault

---

## Validation Rules

### 1. Duplicate Validation

Checks whether duplicate records exist in the file.

### 2. Schema Validation

Validates incoming file columns against metadata stored in Azure SQL Database.

### 3. Date Format Validation

Validates date columns using formats defined in the metadata table.

---

## Metadata Table Structure

| FileName | ColumnName | ColumnDateFormat |
|-----------|-----------|------------------|
| Product | StartDate | yyyy-MM-dd |
| Product | EndDate | yyyy-MM-dd |

---

## Folder Structure

input/
├── landing/
├── staging/
└── rejected/

---

## Features

- Metadata-driven validation
- Dynamic rule management
- Automated file routing
- Scalable architecture
- Reusable validation framework

---

## Business Benefits

- Improves data quality
- Reduces manual validation effort
- Prevents invalid data from entering downstream systems
- Easily supports new file types through metadata changes

---

## Author

Pashvitha Mathi
