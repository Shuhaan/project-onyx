# Northcoders Final Project Discussion 09-08-2024

| Team Member Name | Strengths | Weaknesses |
| --- | --- | --- |
| @Hasan Malik  | Python, TF, AWS | Patching and mocking, SQL |
| @Ewan Ritchie  | Python, patching and mocking, details | AWS, seeing the bigger picture in cloud |
| @Shuhaan Miah  | Python, patching and mocking, github Actions | TF  |
| @Ayub Mensah  | Python, AWS | TF |
| @Arif Syed  | Makefile, AWS, TF, Python, github Actions | Patching and mocking |
| @Saif  | Python, patching/mocking, github actions | SQL, pandas, Athena/Glue |

# Stage

## 1: Extract due August 16, 2024

- Every 5 minutes, check and extract from Totesys PostGRES Database → S3 - MAX 30 MINUTES

### Task 1

- Terraform to create infrastructure and Lambda Function
    - Create two S3 buckets for the data, and one bucket for the code itself
        - Ingestion Bucket and Processed Data Buckets to hold data
    - One bucket for Terraform state and one bucket to hold the code

### Task 2

- Lambda function to extract data - initally upload all tables to bucket, later upload modifications of database to bucket
    - Upload as json files
    - Create up-to-date databases on AWS using Glue and Athena (?)
- Extraction triggers message to Cloudwatch
- All errors and status to be logged to Cloudwatch
    - Cloudwatch will send email in case of major error

### Potential Issues

- Database may not change every 5 minutes leading to duplication of ingested data
- Data in database is modified I.e. a transaction gets changed or cancelled
- Need to keep history of every modification and new data in Totesys DB possibly by using timestamps
    - Could be overcome by using the S3 file structure

## 2: Transform

- Transform Lambda function triggers every time there is any new data in the Ingestion bucket

### Task 1

- Use Lambda to take data from the Ingestion Bucket
- Lambda then transforms to adhere to warehouse schema
    - The data warehouse format is the parquet format, which enables queries to be run on the data
- Pass the transformed data to the Processed Data Bucket
    - OLTP → OLAP

## 3: Load

- Take data from Processed Data bucket and load into Warehouse
- Data is now in OLAP format and can be queried by Business Intelligence or Data Science tools
    - Other tools may include Jupyter notebooks and/or pandas

### Task 1

- Take the processed data and load into the Warehouse for querying

## 4: Test Data

- August 12, 2024 9:19 AM Data can be tested and checked as to suitability for analytics

# To be Done August 12, 2024

- Catch up @Saif
    - Give IAM access information
- Connect to AWS
- Move
- First standup

# Sprint Events

- August 16, 2024 evening retro (repeated every Friday evening for an hour)
- Standup every morning at 0845

# Links

[Northcoders Project Kanban Board](https://www.notion.so/Northcoders-Project-Kanban-Board-cc85ac669f854581ba87d8cbf531b48b?pvs=21)

[Code Documentation](https://www.notion.so/Code-Documentation-35479c48a1474f34ac91c37a5c8bd97c?pvs=21)

[Software Development Tools](https://www.notion.so/Software-Development-Tools-86e831911d65424a9740b91e9a455ffe?pvs=21)

[Comms/Org Tools](https://www.notion.so/Comms-Org-Tools-974926ec72a043e3b26e78d0812d8639?pvs=21)

[Useful Sprints](https://www.notion.so/Useful-Sprints-52207f7baaab4d57845e9f3950fc8e6e?pvs=21)