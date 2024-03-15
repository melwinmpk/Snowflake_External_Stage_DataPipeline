USE AMAZONEBOOKS_EXTERNAL;

USE SCHEMA PUBLIC;

CREATE OR REPLACE STAGE amazonebook_reviews_ext_stage
url='s3://lz-snowflake/Snowflake/amazonebooks_external/amazonebook_reviews/'
file_format = (type = 'CSV' field_delimiter = ',' field_optionally_enclosed_by = '"' skip_header = 1)
STORAGE_INTEGRATION = ACCESS_TO_S3BUCKET;

CREATE OR REPLACE EXTERNAL TABLE AMAZONEBOOK_REVIEWS_EXT(
	BOOK_ID NUMBER(38, 0) AS (value:c1::INTEGER)
	,REVIEWER_NAME VARCHAR(16777216) AS (value:c2::TEXT)
	,RATING FLOAT AS (value:c3::FLOAT)
	,REVIEW_TITLE VARCHAR(16777216) AS (value:c4::TEXT)
	,REVIEW_CONTENT VARCHAR(16777216) AS (value:c5::TEXT)
	,REVIEWED_ON VARCHAR(16777216) AS (value:c6::TEXT)
	,BUSINESS_DATE DATE AS TO_DATE(value:c7::VARCHAR,'YYYY-MM-DD')
    ,FILE_DATE_PARTITION NUMBER(10,0) as (split_part(METADATA$FILENAME,'/',4)::int)
)
PARTITION BY (FILE_DATE_PARTITION)
WITH LOCATION = @amazonebook_reviews_ext_stage
FILE_FORMAT = (TYPE = CSV field_delimiter = ',' field_optionally_enclosed_by = '"'  SKIP_HEADER = 1);

CREATE TRANSIENT TABLE amazonebook_reviews (
    BOOK_ID INTEGER
	,REVIEWER_NAME TEXT
	,RATING FLOAT
	,REVIEW_TITLE TEXT
	,REVIEW_CONTENT TEXT
	,REVIEWED_ON DATE
    ,BUSINESS_DATE DATE
    ,FILE_DATE_PARTITION NUMBER
);


INSERT INTO CONFIG.TABLE_CONFIG VALUES ('amazonebook_reviews','2024-01-01');

INSERT INTO CONFIG.TBL_PRIMARY_KEY VALUES 
('amazonebook_reviews','BOOK_ID',1,1),
('amazonebook_reviews','REVIEWER_NAME',2,2),
('amazonebook_reviews','BUSINESS_DATE',3,7);

