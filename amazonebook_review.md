<H2>Source System</H2>

Airflow Dag (Snowflake_ExternalStage_source_amazonebook_review_Dag) is developed to upload the Data to S3 bucket. Respective config entries for the Tables are updated.

S3 Bucket Location => s3://lz-snowflake/Snowflake/amazonebooks_external/amazonebook_reviews/

<img width="651" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/55a7f1ab-a185-48f3-a35a-605d3ca89a79">
<img width="1208" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/0e567dfe-bfb4-4c52-801f-cd1c686e52e6">
<h2>Target System</h2>
<p>Airflow Dag (Snowflake_ExternalStage_destination_amazonebook_review_Dag) is developed to check any new files arrived in the mentioned s3 bucket location by getting the last extract date for the respective table from the config table.
S3_file_check waits for the File to arrive till the mentioned duration. Once the File arrives the data is first loaded into the External Table and then loaded incrementally to the Snowflake Table. On Succesful Data ingestion the respective Config record is updated</p><br>
<img width="1100" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/467923ef-d087-4014-affb-7e7be3223baa">
<img width="1212" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/f1c99eee-714f-447c-bb62-251d16f8800c">

<H3>ScreenShots</H3>
<H4>S3 Bucket Location</H4>
<img width="406" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/31e6d97f-aa84-445e-8113-53c82c09e061">
<H4>Data in Snowflake Table</H4>
<img width="787" alt="image" src="https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/assets/25386607/3aacce52-9c91-45e8-ac8d-9f56800f4c53">

