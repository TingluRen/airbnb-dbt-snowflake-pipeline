# Airbnb End-to-End Data Engineering Project

## Overview
This project demonstrates a modern data engineering pipeline built using dbt and Snowflake, following a medallion architecture (Bronze, Silver, Gold).

It includes both:
- A **One Big Table (OBT)** for ad-hoc querying
- A **Star Schema (fact + dimensions)** for scalable analytics


## Architecture
AIRBNB
├── BRONZE (raw ingestion)
├── SILVER (cleaned & standardized)
├── GOLD (fact + dimensions + OBT)
├── SNAPSHOTS (SCD Type 2 history tracking)


## Key Features

- Built using **dbt + Snowflake**
- Implemented **metadata-driven pipeline (Jinja + configs)**
- Designed **Star Schema**
  - `fact_bookings`
  - `dim_hosts`
  - `dim_listings`
  - `dim_date`
- Built **OBT (One Big Table)** for analytics
- Implemented **SCD Type 2 snapshots**
- Used `ref()` for dependency management
- Layered architecture: Bronze → Silver → Gold



## Data Models

### Fact Table
- `fact_bookings`
  - booking grain
  - measures: total_amount, service_fee, cleaning_fee

### Dimension Tables
- `dim_hosts`
- `dim_listings`
- `dim_date`

### Snapshot Tables
- `scd_dim_hosts`
- `scd_dim_listings`
- `scd_fact_bookings`



## How to Run
dbt run
dbt snapshot


## Tech Stack
Snowflake
dbt
SQL
Jinja