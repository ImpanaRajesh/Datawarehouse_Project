Overview
This project involves designing and implementing a data warehousing solution for Formula 1 (F1) racing data.
The goal is to integrate diverse F1 datasets, transform them into a dimensional model, and demonstrate analytical capabilities through meaningful queries and visualizations. 
The project is divided into the following phases:

1)Data Sourcing
2)Normalized Database Design
3)ETL Implementation
4)Dimensional Modeling
5)Analytical Querying
6)Visualization

Project Structure
The project is organized into the following folders and files:
-- Raw Data Folder consists of raw data sources
--ELT.text consists of the commands to be executed in snowflake
--Visualisation.py consists of python script file for creating visualisations for the analytical queries
--Visualisation-Images consists of all images for the analytical queries

Setup Instructions

1. Prerequisites
Before running the project, ensure you have the following installed:
Snowflake Account: A Snowflake account with the necessary permissions to create databases, schemas, and tables.
Python: Python 3.8 or higher.
Python Libraries: Install the required libraries using the following command:
'pip install snowflake-connector-python pandas matplotlib seaborn squarify'

2. Snowflake Setup
--Create a Database and Schema:
Log in to your Snowflake account.
Run the following SQL commands to create a database and schema:

CREATE DATABASE F1_DATA_WAREHOUSE;
USE DATABASE F1_DATA_WAREHOUSE;
CREATE SCHEMA STAGING;
CREATE SCHEMA NORMALIZED;
CREATE SCHEMA DIMENSIONS;
CREATE SCHEMA FACTS;

--Create a Warehouse
CREATE WAREHOUSE F1_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

3.ETL Workflow
Execute all the statements in the ETL.txt file in a Snowflake environment

Execute ETL Scripts: Run the ETL scripts (ETL.txt) to transform and load data into the dimensional model.
Handle SCD: Ensure Slowly Changing Dimensions (SCD) Type 2 is implemented for DIM_CONSTRUCTORS and DIM_DRIVERS.

4. Generate Visualizations
Run Visualization Script:
Navigate and run the visualizations.py script to generate visualizations:
'python scripts/visualizations.py'

View Visualizations: The visualizations will be displayed inline (if using Jupyter Notebook) or saved as image files in the analytical_queries/visualizations folder.

5. Usage
Analytical Queries: Run the SQL queries in the etl.txt file to extract insights from the dimensional model.
Visualizations: Use the Python script in the script(Visualisation.py) to generate visualizations for the query results.
