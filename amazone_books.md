<h2>Source System</h2>
<img width="800" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/0f4df5d8-d40f-4c9e-9aa5-1d0380733607">
<p> Airflow Dag (Snowflake_ExternalStage_amazone_books_Dag) is developed to upload the Data to S3 bucket</p>
<p>S3 Bucket Location => s3://lz-snowflake/Snowflake/amazonebooks_external/amazone_books/</p>
<p>as soon as file is loaded to S3 Bucket the AWS Lambeda is Triggered the script <a href='https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/blob/main/lambda_script.py'>lambda_script.py</a> is executed which ingest the data from S3 bucket to Snowflake
<h2>Target System</h2>
<img width="1050" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/8e0e1578-4591-42ee-9968-5d9083b6e0c9">