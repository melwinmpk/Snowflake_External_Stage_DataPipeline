# Snowflake_External_Stage_DataPipeline

<H2>Project Overview</H2>
<p>The "Snowflake External Stage Data Pipeline" project automates the data ingestion process from a MySQL database to Snowflake using an External Stage. 
  This project leverages AWS S3 as an intermediary storage solution, showcasing two distinct pipelines for loading data into Snowflake: one utilizing AWS Lambda and the other using Apache Airflow. Incremental loading is a critical feature for efficiently managing data pipelines, as it ensures that only new or modified data is transferred, saving on both processing time and costs.  This approach allows for flexibility and demonstrates various methods of leveraging cloud services for efficient data management.</p>


<H3>Features</H3>
<ul>
<li><b>External Staging on AWS S3:</b> Data is initially moved from MySQL to an S3 bucket, serving as an external stage for Snowflake.</li>
<li><b>Lambda-Based Pipeline:</b> Utilizes AWS Lambda for serverless data loading from S3 to Snowflake, optimizing for efficiency and cost.</li>
<li><b>Airflow-Based Pipeline:</b> Leverages Apache Airflow for orchestrated data transfer, providing robust scheduling and monitoring capabilities.</li>
<li><b>Incremental Data Loading:</b> Implements an incremental loading approach, ensuring only new or updated records are transferred, optimizing the data ingestion process.</li>
<li><b>Metadata-Driven Ingestion:</b> Uses metadata to dynamically manage data flows, enhancing the flexibility and scalability of the pipeline.</li>
</ul>

<h3>Technologies Used</h3>
<ul>
<li><b>MySQL:</b> Source database for data ingestion.</li>
<li><b>Snowflake:</b> Cloud data warehouse for data storage and analysis.</li>
<li><b>AWS S3:</b> Used as external staging for data before loading into Snowflake.</li>
<li><b>AWS Lambda:</b> Serverless computing service for executing data loading scripts.</li>
<li><b>Apache Airflow:</b> Workflow automation and scheduling tool that orchestrates the data pipeline.</li>
<li><b>Python:</b> Primary programming language for scripting and automation.</li>
</ul>

<h3>How It Works</h3>
<ul>
<li>Data is extracted incrementally from MySQL and loaded into an S3 bucket.</li>
<li>Two separate pipelines are defined for data loading into Snowflake:</li>
  <ul>
  <li><b>Lambda Pipeline:</b> Triggered upon new data arrival in S3, executing a script to load data into Snowflake.</li>
  <li><b>Airflow Pipeline:</b> Scheduled tasks for data extraction, loading to S3, and subsequent loading into Snowflake using External Stage.</li>
  </ul>
<li>Metadata-driven approach allows for dynamic adaptation of the data ingestion process as source data evolves.</li>
</ul>

<h3>Future Enhancements</h3>
<ul>
<li>Explore additional optimizations for handling larger datasets.</li>
<li>Implement error handling and retry mechanisms for increased reliability.</li>
<li>Enhance monitoring and alerting for pipeline operations.</li>
</ul>

<H3>Source Data</H3>
<p>There are 2 tables which are getting ingested to Snowflake.</p>
<p>Dedicated Dags are developed for each Table</p>
<ol>
	<li>amazone_books</li>
	<li>amazonebook_reviews </li>
</ol>
<h4>Source Table DDls</h4>
<pre>
CREATE TABLE amazone_books (
	book_id INT NOT NULL AUTO_INCREMENT
	,book_title TEXT
	,book_amount FLOAT
	,book_author TEXT
	,book_rating FLOAT
	,book_link TEXT
	,business_date DATE DEFAULT(CURRENT_DATE)
	,PRIMARY KEY (book_id)
	);

CREATE TABLE amazonebook_reviews (
	book_id INT NOT NULL
	,reviewer_name TEXT
	,rating FLOAT
	,review_title TEXT
	,review_content TEXT
	,reviewed_on DATE
 	,business_date DATE DEFAULT(CURRENT_DATE)
	);
</pre>
<p> For the Incremental load. Primary Keys are required in the Tables. Respective Primary key for the Table are</p>
<ul>
	<li>amazone_books</li>
	<ul>
		<li>book_id</li>
	</ul>	
	<li>amazonebook_reviews</li>
	<ul>
		<li>book_id</li>
		<li>reviewer_name</li>
		<li>business_date</li>
	</ul>
		
</ul>	
<p> Note: This Source Data is from another Project. To know more about how source data is generated please refer <a href='https://github.com/melwinmpk/AmazonBooks_DataPipeline?tab=readme-ov-file#amazonbookdata_datapipeline'>AmazonBooks_DataPipeline</a>  </p>
<p>The Airflow Dag Ids for respective Tables are</p>
<ul>
	<li>amazone_books</li>
	<ul>
		<li>Snowflake_ExternalStage_amazone_books_Dag</li>
	</ul>	
	<li>amazonebook_reviews</li>
	<ul>
		<li>Snowflake_ExternalStage_source_amazonebook_review_Dag</li>
		<li>Snowflake_ExternalStage_destination_amazonebook_review_Dag</li>
	</ul>	
</ul>

<H3>For Data Ingesition Using AWS Lambda Services Please refer <a href='https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/blob/main/amazone_books.md'>amazone_books.md</a> </H3>
<H3>For Data Ingesition Using Airflow Services Please refer <a href='https://github.com/melwinmpk/Snowflake_External_Stage_DataPipeline/blob/main/amazonebook_review.md'>amazonebook_review.md</a> </H3>

<h2>Source System</h2>
<img width="800" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/0f4df5d8-d40f-4c9e-9aa5-1d0380733607">
<h2>Target System</h2>
<img width="1050" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/8e0e1578-4591-42ee-9968-5d9083b6e0c9">

<h2>Bookreview Data</h2>
<img width="1100" alt="image" src="https://github.com/melwinmpk/AmazonBooks_DataPipeline/assets/25386607/467923ef-d087-4014-affb-7e7be3223baa">
