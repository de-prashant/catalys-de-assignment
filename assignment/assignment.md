# Data_Engineering_Assignment_ETL_and_Data_Modeling

*Document Type: DOCX*

## Table of Contents

- [Data Engineering Assignment – ETL & Data Modeling](#data-engineering-assignment--etl--data-modeling)
  - [Objective](#objective)
  - [Problem Statement](#problem-statement)
  - [ETL Requirements](#etl-requirements)
  - [Data Modeling Requirements](#data-modeling-requirements)
  - [SQL Expectations](#sql-expectations)
  - [Data Quality & Reliability](#data-quality--reliability)
  - [Deliverables](#deliverables)
  - [Submission Guidelines](#submission-guidelines)

# Data Engineering Assignment – ETL & Data Modeling

## Objective

Design and implement a small-scale data engineering solution that demonstrates hands-on experience with ETL pipelines, relational and NoSQL data stores, and data transformation using SQL. The assignment focuses on practical decision-making, data modeling, and pipeline reliability.

## Problem Statement

You are given raw data from multiple sources that need to be ingested, transformed, and stored for analytics and reporting purposes.Sources:1. Transactional data (structured, relational)2. Event or log data (semi-structured or NoSQL)The goal is to build an end-to-end ETL pipeline that prepares clean, query-ready datasets.

## ETL Requirements

Ingestion:- Ingest data from at least two different source types- Support incremental or delta loadsTransformation:- Clean and standardize data- Handle missing or invalid records- Apply business-level transformations using SQLLoad:- Load processed data into a relational database- Store raw or semi-processed data in a NoSQL store

## Data Modeling Requirements

Relational Database:- Design normalized or analytics-friendly tables- Clearly define primary and foreign keysNoSQL Database:- Choose an appropriate data model- Justify the choice of document, key-value, or column-based structureExplain the reasoning behind choosing SQL vs NoSQL for each dataset.

## SQL Expectations

- Write transformation queries using joins and aggregations- Use window functions where applicable- Demonstrate performance-aware SQL design- Provide example queries used in the pipeline

## Data Quality & Reliability

- Basic validation checks (nulls, duplicates, schema issues)- Error handling and logging approach- Idempotent pipeline behavior where possible

## Deliverables

- ETL pipeline implementation (ADF pipelines or equivalent)- SQL scripts used for transformations- Sample input and output datasets- Architecture or flow explanation (text or diagram)- README explaining setup and design decisions

## Submission Guidelines

Submit the assignment as a public GitHub repository.The repository should include:- Clear README- Steps to run or simulate the pipeline- Assumptions and limitationsIf cloud services are used, mock or sample configurations are acceptable.


