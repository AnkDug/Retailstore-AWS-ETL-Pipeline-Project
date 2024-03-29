# Retailstore-AWS-ETL-Pipeline-Project

### PROJECT OVERVIEW : Designed and developed a ETL data pipeline for a retail store using AWS services. The pipeline collects data from the store's transactional database (OLTP) in Snowflake and transforms the raw data using Apache Spark to meet the business requirements. The pipeline also enables Data Analysts to run SQL queries using Athena and create data visualizations using Superset. The batch ETL pipeline is orchestrated using Airflow, an open-source platform to programmatically author, schedule, and monitor workflows. The whole project runnig on EC2 instance(virtual machine) to get hands on experience of AWS cloud service.

##  Project Architecture

![Architecture ](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/3ddea1a0-9f13-46bd-8b93-6fa023d11491)


## 1. Business Scenario
A Retail Store requires a Data engineer to build a data pipeline (ETL) that extract raw data from the organization database, transforms the data to satisfy buisness  requirements and  provides a platform for Data Analyst to generate visualisations to answer some business questions. The data model for the OLTP database is shown below <img src="https://github.com/Joshua-omolewa/Retailstore_ETL_pipeline_project/blob/main/img/Data%20Model%20for%20Database.jpg"  width="100%" height="100%">



## 2. Business Requirements
The data engineer is required to produce a weekly table that meets the following requirements: This table will enable Data Analyst to answer business questions.

The table will be grouped by each week, each store, each product to calculate the following metrics: (**I translated the business requirement to mini SQL statement I will need during the transformation process using Spark**)

* total quantity of  products : **Sum(sales_qty)**
* total sales amount of products : **Sum(sales_amt)**
* average sales Price: **Sum(sales_amt)/Sum(sales_qty)**
* stock level by then end of the week : **stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)**
* store on Order level by then end of the week: **ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week)**
* total cost for the week: **Sum(cost_amt)**
* the percentage of Store In-Stock: **(how many times of out_of_stock in a week) / days of a week (7 days)**
* total Low Stock Impact: **sum (out_of+stock_flg + Low_Stock_flg)**
* potential Low Stock Impact: **if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)**
* no Stock Impact: **if out_of_stock_flg=true, then sum(sales_amt)**
* low Stock Instances: **Calculate how many times of Low_Stock_Flg in a week**
* no Stock Instances: **Calculate then how many times of out_of_Stock_Flg in a week**
* how many weeks the on hand stock can supply: **(stock_on_hand_qty at the end of the week) / sum(sales_qty)**

## 3. STEPS USED TO COMPLETE THE PROJECT 

### Please note: Python language & SQL are used to build the pyspark script that utilizes the SparkSQL API for transforming the raw data to meet the business requirement using the Amazon EMR cluster. 

* I created a database with schema in Snowflake using this   **[SQL DDL script](Data_dump_file.txt)**  and then I loaded the **[raw data](https://drive.google.com/drive/folders/1TL3mtDTW4Uv59cyp3C9COgVgGMaBEImB?usp=sharing)** which is another s3 bucket into the Snowflake database by creating integration stage of snowflake using this **[integration procedure](S3_integration_stage.txt)** and s3 bucket in order to setup the Snowflake OLTP system.
![Snowflake_code](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/4c14b063-dbd8-4364-89d0-40b927d84b38)

![snowflake_code2](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/3b89bb69-7ed0-42e7-9719-6ad1f8f6dc83)

* I Wrote a SQL [stored procedure](Data_loading_procedure.txt) that would initiate the extraction of the raw data into the staging s3 bucket (i.e., input s3 bucket) every day at 12:00 MST, which is the off peak period for the retail store. Snowflakes allows the creation of tasks that utilise chron to run any query. I create a task that runs the store procedure to load the data for each day to the input s3 bucket at  12:00 am MST everyday. This process creates a batch load of raw data at the end of each day to be moved into the staging s3 bucket
  * store procedure sql code
```
--Step 1. Create a procedure to load data from Snowflake table to S3 using SQL format.
CREATE OR REPLACE PROCEDURE COPY_INTO_S3()
-- specifying what data the return value should have
RETURNS DATE
-- Using sql language
LANGUAGE SQL 
AS
$$
BEGIN 
-- First step is to create a dat2 variable that stores  current date function on snowflake  which has YYYY-MM-DD format by default
	LET dat2 STRING := (SELECT CURRENT_DATE());	

--For 1st Table to be loaded to S3 following the step below
-- Next step  is to create a QUERY variable that concatenate the sql query and variable date and then send it  out to S3 bucket for staging based on date in cal_dt column
	LET QUERY0 STRING := (SELECT CONCAT('COPY INTO @raw_data_stage/inventory_',:dat2,'.csv FROM (select * from project_db.raw.inventory where cal_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION=\'NONE\') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE'));
--  NEXT STEP IS TO EXECUTE our query string above using EXECUTE IMMEDDIATE  below to start the staging process	
	EXECUTE IMMEDIATE (:QUERY0);


--For 2nd Table to be loaded to S3 following the step below
-- Next step is to create a QUERY variable that concatenate the sql query and variable date and then send it out to  S3 bucket for staging based on date in trans_dt column
	LET QUERY1 STRING := (SELECT CONCAT('COPY INTO @raw_data_stage/sales_',:dat2,'.csv FROM (select * from project_db.raw.sales where trans_dt <= current_date()) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION=\'NONE\') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE'));
--  NEXT STEP IS TO EXECUTE our query string above using EXECUTE IMMEDDIATE  below to start the staging process	
	EXECUTE IMMEDIATE (:QUERY1);

--For 3rd Table to be loaded to S3 following the step below
-- Next step is to create a QUERY variable that concatenate the sql query and variable date and then send it out to S3 bucket for staging
	LET QUERY2 STRING := (SELECT CONCAT('COPY INTO @raw_data_stage/store_',:dat2,'.csv FROM (select * from project_db.raw.store ) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION=\'NONE\') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE'));
--  NEXT STEP IS TO EXECUTE our query string above using EXECUTE IMMEDDIATE  below to start the staging process	
	EXECUTE IMMEDIATE (:QUERY2);

--For 4th Table to be loaded to S3 following the step below
-- Next step is to create a QUERY variable that concatenate the sql query and variable date and then send it out to S3 bucket for staging
	LET QUERY3 STRING := (SELECT CONCAT('COPY INTO @raw_data_stage/product_',:dat2,'.csv FROM (select * from project_db.raw.product ) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION=\'NONE\') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE'));
--  NEXT STEP IS TO EXECUTE our query string above using EXECUTE IMMEDDIATE  below to start the staging process	
	EXECUTE IMMEDIATE (:QUERY3);

--For 5th Table to be loaded to S3 following the step below
-- Next step is to create a QUERY variable that concatenate the sql query and variable date and then send it  out to S3 bucket for staging
	LET QUERY4 STRING := (SELECT CONCAT('COPY INTO @raw_data_stage/calendar_',:dat2,'.csv FROM (select * from project_db.raw.calendar) file_format=(FORMAT_NAME=CSV_COMMA, TYPE=CSV, COMPRESSION=\'NONE\') SINGLE=TRUE HEADER=TRUE MAX_FILE_SIZE=107772160 OVERWRITE=TRUE'));
--  NEXT STEP IS TO EXECUTE our query string above using EXECUTE IMMEDDIATE  below to start the staging process	
	EXECUTE IMMEDIATE (:QUERY4);

-- RETURNING DATE (OPTIONAL) TO CHECK EVERYTHING IS WORKING
	RETURN dat2;
END;   
$$
;
```
*
  * sql code used to create task and activate task (This task runs the stored procedure)

 ```
  --Step 2. Create a task to run the job. Here we use cron to set job at 12am MST everyday. 
CREATE OR REPLACE TASK load_data_to_s3
WAREHOUSE = PROJECT 
SCHEDULE = 'USING CRON 0 12 * * * America/Edmonton'
AS
CALL COPY_INTO_S3();

--Step 3. Activate the task
ALTER TASK load_data_to_s3 resume;
```
* I created an Input S3 (Amazon Simple Storage Service) bucket as a staging area for the raw data coming from the Snowflake. The raw data is extracted from the snowflake database using store procedure. Sample raw data from in the s3 bucket is shown in the image below. ![s3](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/ba212947-8524-4118-bdf3-d36b4000ef4a)

* Utilized  Cloudwatch to pre-set a rule that automatically triggers the Lambda function at 12:05am MST as raw data extracted daily from snowflake  is expected in the s3 staging area by 12:00 a.m. MST .

* I created the lambda function that cloudwatch triggers at 12:05 a.m. MST that scans the input s3 bucket (staging area) for yesterday's raw data and if found, the lambda function triggers the airflow workflow automatically to start the batch processing. Also, if the data is not yet available, an email is sent to notify the data engineer that the data from the transactional database has not been received. The python code for the lambda function can be found  **[here](lambda_function.py)** while the email python code for the lambda function can be found **[here](send_email.py)** . Please note that Amazon SES is needed to setup the email notification
![lambda](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/1bb7afe7-ddcb-424e-8ea2-38937836cd9a)

* I created docker containers running Airflow using docker compost  within an EC2 (Amazon Elastic Compute Cloud) instance. Ariflow is used to orchestrate & schedule the the movement of data from S3 to the Amazon EMR cluster for transformation. Airflow submits the spark job to the EMR cluster, and also monitor the spark job transformation step in the EMR cluster and displays if the EMR steps were executed successfully in the DAG Graph view. The airflow python code for the airflow dags can be found **[here](a_midterm_dag.py)**
![airflow1](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/c7074ed0-a8a8-4144-aa2b-0040635cb63b) ![airflow2](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/0afa2b9c-2c39-4d3c-a2e8-fec267b97362)

* I created an Amazon EMR cluster through the same dag file named a_midterm_dag. The EMR will transform the raw data to meet the business requirement using pyspark code & spark submit configuration received from airflow and load the transformed data in parquet format to the output S3  bucket (ETL process). The pyspark scripts for the project can be found  **[here](etl_pyspark_script.py)**
![emr](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/672063df-9bda-496d-af9e-48e7dde40b18)

* I created an output S3 bucket in which the transformed data from the EMR Cluster is stored. ![s3_output](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/f4aa293f-38c0-4550-8398-bcf4d36b16bf)

* Utilised Glue to automatically crawl the output S3 bucket to create tables in the Glue catalogue/database that can be queried using Athena by the data analyst.
* Made use of Athena to query the tables created using Glue. The Data anlyst can interact with the weekly fact table using SQL in order to answer business questions.
![athena](https://github.com/AnkDug/Retailstore-AWS-ETL-Pipeline-Project/assets/55326423/3c0af81d-9967-47fd-9221-da234e3eb56a)

* Finally, I created a connection between Athena and power BI using Athena ODBC connector. Power BI  can be used to create data visualisations  & dashboards  by the Data Analyst. It extracts data from Anthena tables, which enables  the Data Analyst to answer any business question.

## Refrences:
Snowflake S3 Integration documentation: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
Aiflow Dag Documentation: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
Athena Power BI connection: https://www.youtube.com/watch?v=WS3LUbK0ung
## Show your support

Give a ⭐️ if this project helped you!











