# Restaurant Visit Management System

## Overview
This project is a **Restaurant Visit Management System** that stores and manages restaurant visit data, including customer details, server assignments, billing, and payments. The system utilizes **AWS RDS (Relational Database Service)** with a structured relational database containing **10+ tables** to efficiently handle restaurant operations.

## Features
- **Database Design**: Created a relational database with **10+ tables** to manage restaurants, visits, customers, servers, payments, and billing.
- **AWS RDS Integration**: Used **AWS RDS (PostgreSQL/MySQL)** for cloud-based database management.
- **Data Processing Pipelines**: Automated data cleaning, transformation, and ingestion into AWS RDS.
- **Stored Procedures & Queries**: Implemented SQL stored procedures for automated visit and billing entries.
- **Data Validation & Reconciliation**: Ensured data accuracy by cross-checking source files with database records.
- **Batch Processing**: Optimized data insertion with batch execution for efficient database operations.

## Technologies Used
- **Database**: AWS RDS (MySQL/PostgreSQL)
- **Programming Language**: R (with DBI & RMySQL/RPostgreSQL packages)
- **Data Processing**: dplyr, readr, lubridate
- **SQL Features**: Stored Procedures, Foreign Keys, Joins, Indexing

## Database Schema
The database consists of **10+ tables**, including:
- `RESTAURANT`: Stores restaurant details.
- `CUSTOMER`: Maintains customer profiles.
- `SERVER`: Stores server (employee) details.
- `VISIT`: Logs customer visits, linked to restaurants and servers.
- `BILL`: Records payment details for each visit.
- `PAYMENTMETHOD`: Stores different payment methods.

## Setup & Installation
### 1. Database Setup (AWS RDS)
1. **Create AWS RDS Instance**
   - Choose **MySQL/PostgreSQL** as the database engine.
   - Configure instance settings (allocate storage, set credentials, enable public access if needed).
   - Obtain the **endpoint URL** for database connection.

2. **Create Tables**
   - Use the provided SQL schema script (`schema.sql`) to create database tables.

### 2. Running the Project
1. Install required R packages:
   ```r
   install.packages("DBI")
   install.packages("RMySQL") # or install.packages("RPostgreSQL") for PostgreSQL
   install.packages("dplyr")
   install.packages("readr")
   install.packages("lubridate")
   ```

2. Configure database connection in R:
   ```r
   con <- dbConnect(RMySQL::MySQL(),
                    host = "your-rds-endpoint",
                    user = "your-username",
                    password = "your-password",
                    dbname = "defaultdb",
                    port = 3306) # Use 5432 for PostgreSQL
   ```

3. Run the data loading scripts in R:
   ```r
   source("load_data.R")
   ```

### 3. Running Validation Tests
Execute the `testDBLoading.R` script to verify successful data loading:
```r
source("testDBLoading.R")
```

## Future Enhancements
- Implement a **web interface** for real-time restaurant visit tracking.
- Add **role-based authentication** for secure database access.
- Enhance **reporting and analytics** with visual dashboards.

## Author
**Pratosh Karthikeyan**  
**pratosh2002@gmail.com**  

For any questions or contributions, feel free to reach out!

