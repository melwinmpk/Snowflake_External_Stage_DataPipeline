<h2>Source System</h2>
<p> Airflow Dag (Snowflake_ExternalStage_amazone_books_Dag) is developed to upload the Data to S3 bucket. Respective config entries for the Tables are updated.</p>
<p>S3 Bucket Location => s3://lz-snowflake/Snowflake/amazonebooks_external/amazone_books/</p>
<img width="800" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/0f4df5d8-d40f-4c9e-9aa5-1d0380733607"><br>

<h2>Target System</h2>
<p>As soon as the file is loaded to S3 Bucket the AWS Lambeda is Triggered. The script <a href='https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/blob/main/lambda_script.py'>lambda_script.py</a> is executed which ingests the data from S3 bucket to Snowflake.
Data is Loaded Incrementally from the External Table linked to the External Stage Table that points to the S3 bucket location to the Snowflake Table.
  Finally, respective config entries for the Tables are updated.</p>
<img width="1050" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/8e0e1578-4591-42ee-9968-5d9083b6e0c9">

<H3>Screen Shots</H3>
<h4>S3 bucket Location</h4>
<img width="580" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/408801de-f9e2-429a-b7a8-ace0b930a9dd">
<h4>Data in Snowflake Table</h4>
<img width="780" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/e026f8da-af20-4eb0-a4b8-9af0c6e98c76">
