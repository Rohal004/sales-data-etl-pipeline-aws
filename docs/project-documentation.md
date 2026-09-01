# Sales Data ETL Pipeline — Project Documentation

## 1. Project Overview

This project is an AWS-based Sales Data ETL Pipeline designed to ingest raw sales CSV files, validate and transform the data, convert it into an optimized Parquet format, and make it available for analytical queries using Amazon Athena.

The pipeline also handles data-quality problems such as duplicate records, missing values, invalid prices, invalid quantities, and invalid dates.

---

## 2. Project Objectives

The main objectives of the project are:

- Ingest sales data through Amazon S3.
- Process and transform data using AWS Lambda.
- Validate incoming sales records.
- Detect duplicate records.
- Detect missing values.
- Validate prices and quantities.
- Validate order dates.
- Separate valid and invalid records.
- Convert processed CSV data to Parquet.
- Apply Snappy compression.
- Partition processed data by year and month.
- Catalog the processed data using AWS Glue.
- Query the data using Amazon Athena.
- Monitor the ETL pipeline using CloudWatch.

---

## 3. Architecture

The pipeline follows this flow:

Sales CSV
    ↓
Amazon S3 Raw Bucket
    ↓
AWS Lambda ETL Processor
    ↓
Validation and Transformation
    ↓
Valid Records → Parquet
Invalid Records → Rejected CSV
    ↓
Amazon S3 Processed Bucket
    ↓
AWS Glue Crawler
    ↓
AWS Glue Data Catalog
    ↓
Amazon Athena
    ↓
SQL Analytics

CloudWatch Logs and Lambda Metrics are used for monitoring.

The architecture diagram is available at:

architecture/aws-sales-etl-architecture.png

---

## 4. AWS Services Used

### Amazon S3

Two S3 buckets are used:

- Raw/input bucket
- Processed/output bucket

The raw bucket receives sales CSV files.

The processed bucket stores transformed Parquet files and rejected records.

---

### AWS Lambda

The Lambda function:

Sales-ETL-Processor

is responsible for the main ETL processing.

It performs:

- File processing
- Schema validation
- Duplicate detection
- Data validation
- Transformation
- CSV-to-Parquet conversion
- Snappy compression
- Partitioning
- Error handling
- Logging

---

### AWS Glue

AWS Glue is used to crawl the processed Parquet data and create metadata in the Glue Data Catalog.

The crawler used in the project is:

sales-data-crawler

---

### Amazon Athena

Athena is used to query the processed data using SQL.

Partition filtering is used to reduce unnecessary data scanning.

---

### Amazon CloudWatch

CloudWatch is used for:

- ETL logs
- Error logs
- Lambda execution information
- Monitoring Lambda activity

---

## 5. ETL Processing

The pipeline processes incoming records through several stages.

### Step 1 — Ingestion

A sales CSV file is uploaded to the Raw S3 bucket.

### Step 2 — Lambda Trigger

The S3 upload triggers the Lambda ETL processor.

### Step 3 — Schema Validation

The incoming records are checked against the expected sales-data schema.

### Step 4 — Data Validation

Records are checked for:

- Missing values
- Duplicate OrderIDs
- Invalid prices
- Invalid quantities
- Invalid dates

### Step 5 — Record Separation

Valid records continue through the pipeline.

Invalid records are rejected and recorded separately.

### Step 6 — Transformation

Valid records are transformed into the required output structure.

### Step 7 — Parquet Conversion

The valid records are converted from CSV-compatible data into Parquet format.

### Step 8 — Compression

Snappy compression is applied to the Parquet data.

### Step 9 — Partitioning

Processed data is partitioned using:

year=YYYY/month=MM

For example:

processed/parquet/year=2026/month=08/

### Step 10 — Cataloging

AWS Glue discovers the processed data and updates the Data Catalog.

### Step 11 — Analytics

Amazon Athena queries the processed data using SQL.

---

## 6. Data Quality Testing

The pipeline was intentionally tested with several types of bad data.

### Duplicate Records

Duplicate OrderIDs were introduced and detected.

### Missing Values

Missing values were introduced into required fields such as:

- Product
- Quantity
- UnitPrice
- OrderDate
- Country

### Invalid Prices

The pipeline was tested with:

- Negative prices
- Non-numeric prices

### Invalid Quantities

The pipeline was tested with:

- Zero quantities
- Negative quantities
- Non-numeric quantities

### Bad Dates

The pipeline was tested with:

- Incorrect date formats
- Invalid months
- Invalid calendar dates

---

## 7. Data Lake Optimization

The project demonstrates several data-lake optimization concepts.

### CSV vs Parquet

CSV is useful for simple data exchange and ingestion.

Parquet is better suited for analytical workloads because it is a columnar storage format.

### Compression

Snappy compression was used with Parquet to reduce storage and data-reading requirements.

### Partitioning

The processed data is partitioned by year and month.

This allows Athena to use partition pruning when queries include partition filters.

### Athena Query Cost

Athena pricing is related to the amount of data scanned by queries.

Using Parquet, compression, and appropriate partitioning can reduce unnecessary data scanning.

---

## 8. Error Handling

The Lambda function includes error handling so that unexpected processing failures can be logged.

ETL failures are recorded using structured logging.

The project also includes an intentional error test to verify that failures appear in CloudWatch and Lambda monitoring.

---

## 9. Monitoring

The pipeline uses Amazon CloudWatch and Lambda monitoring.

### CloudWatch Logs

Used to inspect individual ETL executions and events.

### Lambda Metrics

Used to monitor:

- Invocations
- Errors
- Duration
- Throttles

### Error Monitoring

Intentional failures were generated to verify that errors could be detected through Lambda metrics and CloudWatch logs.

---

## 10. Project Testing

The pipeline was tested through:

- Successful ETL execution
- S3-triggered processing
- Duplicate-data testing
- Missing-value testing
- Invalid-price testing
- Invalid-quantity testing
- Bad-date testing
- Error-handling testing
- CloudWatch log verification
- Lambda metric verification

---

## 11. Project Outcome

The completed pipeline provides an automated workflow for processing sales data from raw CSV files into optimized, queryable Parquet data.

The project demonstrates practical data-engineering concepts including:

- ETL
- AWS S3
- AWS Lambda
- AWS Glue
- Amazon Athena
- Parquet
- Compression
- Partitioning
- Data validation
- Data-quality handling
- Error handling
- Cloud monitoring