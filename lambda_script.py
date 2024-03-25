import json
import pandas as pd
import boto3
import snowflake.connector

def lambda_handler(event, context):
    AWS_CREDENTAILS = { 
                    "AWS_ACCESS_KEY_ID":"",
                    "AWS_SECRET_ACCESS_KEY":""
                  }
    SNOWFLAKE_CREDENTIALS = {
                            "USER": "MELWINMPK",
                            "PASSWORD": "",
                            "ACCOUNT": "",
                            "WHAREHOUSE": "COMPUTE_WH",
                            "DATABASE":"amazonebooks_external",
                            "SCHEMA":"PUBLIC"
                            }
    MAIN_DATA = { 
                 "database_name":"amazonebooks_external",
                  "Schema":"PUBLIC",
                  "table_name":"amazone_books",
                  "Stage_name":"amazone_books_ext_stage",
                  "External_Table":"AMAZONE_BOOKS_EXT",
                  "load_type":"Incremental",
                  "Bucket":"lz-snowflake",
                  "S3_Path":"Snowflake/"
                }
    # Get Last Extract from Config Table 

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CREDENTIALS["USER"],
        password=SNOWFLAKE_CREDENTIALS["PASSWORD"],
        account=SNOWFLAKE_CREDENTIALS["ACCOUNT"],
        warehouse=SNOWFLAKE_CREDENTIALS["WHAREHOUSE"],
        database=SNOWFLAKE_CREDENTIALS["DATABASE"],
        schema=SNOWFLAKE_CREDENTIALS["SCHEMA"]
        )
    curr = conn.cursor()
    
    snow_query = f'''Select last_extract_date from config.table_config  where table_name = '{MAIN_DATA["table_name"]}';'''
    last_extract_dt_df = pd.read_sql(snow_query, conn)
    
    print(last_extract_dt_df)
    
    last_extract = int((str(last_extract_dt_df["LAST_EXTRACT_DATE"][0])).replace("-","")) 
    
    s3_client = boto3.client("s3",
                             aws_access_key_id=AWS_CREDENTAILS["AWS_ACCESS_KEY_ID"],
                             aws_secret_access_key=AWS_CREDENTAILS["AWS_SECRET_ACCESS_KEY"])
    
    # Get all the new Files that are loaded
    
    current_data_paths = []
    prefix = f'{MAIN_DATA["S3_Path"]}{MAIN_DATA["database_name"]}/{MAIN_DATA["table_name"]}/'
    
    paginator = s3_client.get_paginator("list_objects_v2")
    response = paginator.paginate(Bucket=f'{MAIN_DATA["Bucket"]}', Prefix=prefix)
    for page in response:
        files = page.get("Contents")
        for file in files:
            File_path = file['Key'].replace(prefix,"")
            if last_extract < int(File_path.split("/")[0]):
                current_data_paths.append(File_path)
    print(current_data_paths)
    
    snow_query = f'''ALTER EXTERNAL TABLE IF EXISTS  {MAIN_DATA["External_Table"]} ADD FILES  ( '{"','".join(current_data_paths)}');                          
        '''
    curr.execute(snow_query)
        
    # Getting the Primary Key from the config Table
    
    snow_query = f'''SELECT * FROM CONFIG.tbl_primary_key where table_name = '{MAIN_DATA["table_name"]}';'''
    primary_key_data_df = pd.read_sql(snow_query, conn)
    
    if MAIN_DATA["load_type"] == "Incremental":
        
        '''
        MERGE INTO AMAZONE_BOOKS T1
          USING (SELECT * FROM AMAZONE_BOOKS_EXT WHERE FILE_DATE_PARTITION > 20240226 ) T2 ON T1.BOOK_ID = T2.BOOK_ID 
          WHEN MATCHED THEN UPDATE SET T1.BOOK_TITLE = T2.BOOK_TITLE,
                                        T1.BOOK_AMOUNT = T2.BOOK_AMOUNT,
                                        T1.BOOK_RATING = T2.BOOK_RATING, 
                                        T1.BOOK_LINK = T2.BOOK_LINK,
                                        T1.BUSINESS_DATE = T2.BUSINESS_DATE,
                                        T1.FILE_DATE_PARTITION = T2.FILE_DATE_PARTITION
          WHEN NOT MATCHED THEN INSERT (BOOK_ID, BOOK_TITLE, BOOK_AMOUNT, BOOK_AUTHOR, BOOK_RATING, BOOK_LINK, BUSINESS_DATE , FILE_DATE_PARTITION)
          VALUES (T2.BOOK_ID, T2.BOOK_TITLE, T2.BOOK_AMOUNT, T2.BOOK_AUTHOR, T2.BOOK_RATING, T2.BOOK_LINK, T2.BUSINESS_DATE , T2.FILE_DATE_PARTITION);
        '''
    
        query = f''' MERGE INTO {MAIN_DATA["table_name"]} T1 
                     USING ( 
                         SELECT * FROM {MAIN_DATA["External_Table"]} t 
                         WHERE FILE_DATE_PARTITION > {(str(last_extract_dt_df["LAST_EXTRACT_DATE"][0])).replace("-","")} ) T2 ON '''
        query_part1 = ''
        query_part2 = ''
        query_part3 = ''
        query_part4 = ''
    
        for i in range(primary_key_data_df.shape[0]):
            primary_column = (primary_key_data_df.loc[i,["PRIMARY_COLUMN"]]).item()
            query_part1 += f' T1.{primary_column} = T2.{primary_column} and'
    
        query += f'''{query_part1[:-3]} WHEN MATCHED THEN UPDATE SET '''
        
        snow_query = f''' SHOW COLUMNS IN TABLE {MAIN_DATA["External_Table"]};  '''
        table_columns_df = pd.read_sql(snow_query, conn)
        
        for column in  table_columns_df["column_name"]: 
            if column != 'VALUE':
                query_part2+= f''' T1.{column} = T2.{column},'''
                query_part3+= f''' {column},'''
                query_part4+= f''' T2.{column},'''

        query += f'''{query_part2[:-1]} WHEN NOT MATCHED THEN INSERT ({query_part3[:-1]}) VALUES ({query_part4[:-1]});''' 
        curr.execute(query)
        print(query)
    
    
    # UPDATING THE CONFIG TABLE
    
    query = f''' UPDATE CONFIG.TABLE_CONFIG
                 SET LAST_EXTRACT_DATE = (SELECT MAX(business_date) FROM PUBLIC.{MAIN_DATA["table_name"]})
                 WHERE TABLE_NAME = '{MAIN_DATA["table_name"]}';
    '''
    
    curr.execute(query)
    
    
    conn.close()
    print("Done")
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }