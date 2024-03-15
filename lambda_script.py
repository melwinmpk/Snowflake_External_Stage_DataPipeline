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
                            "ACCOUNT": "bfbuljl-bnb23563",
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
    snow_query = 'USE SCHEMA CONFIG;'
    curr.execute(snow_query)
    
    snow_query = f'''SELECT * FROM CONFIG.tbl_primary_key where table_name = '{MAIN_DATA["table_name"]}';'''
    primary_key_data_df = pd.read_sql(snow_query, conn)
    
    snow_query = f'USE SCHEMA {MAIN_DATA["Schema"]};'
    curr.execute(snow_query)
    
    if MAIN_DATA["load_type"] == "Incremental":
    
        query = f'DELETE FROM  {MAIN_DATA["table_name"]} T1 USING ( SELECT '
        query_part1 = ''
        query_part2 = ''
        query_part3 = ''
    
    
        for i in range(primary_key_data_df.shape[0]):
            
            primary_column = (primary_key_data_df.loc[i,["PRIMARY_COLUMN"]]).item()
    
            query_part1 += f' {primary_column},'
            query_part2 += f' T1.{primary_column},'
            query_part3 += f' T1.{primary_column} = T2.{primary_column} and'
    
        query += (query_part1[:-1] + f''' FROM {MAIN_DATA["External_Table"]} t WHERE FILE_DATE_PARTITION > {(str(last_extract_dt_df["LAST_EXTRACT_DATE"][0])).replace("-","")} ) T2 WHERE {query_part3[:-3]};''')
        curr.execute(query)
        print(query)
    
    snow_query = f''' SHOW COLUMNS IN TABLE {MAIN_DATA["External_Table"]};  '''
    table_columns_df = pd.read_sql(snow_query, conn)
    
    # INSERTING THE NEW DATA TO THE TABLE
    table_columns_strig =  ','.join([ i for i in table_columns_df["column_name"] if i != 'VALUE' ])
    
    snow_query = f''' INSERT INTO {MAIN_DATA["table_name"]} 
                      (SELECT {table_columns_strig} FROM {MAIN_DATA["External_Table"]} WHERE FILE_DATE_PARTITION > {(str(last_extract_dt_df["LAST_EXTRACT_DATE"][0])).replace("-","")});'''
                      
    print(snow_query)
    
    curr.execute(snow_query)
    
    snow_query = 'USE SCHEMA CONFIG;'
    curr.execute(snow_query)
    
    query = f''' UPDATE CONFIG.TABLE_CONFIG
                 SET LAST_EXTRACT_DATE = (SELECT MAX(business_date) FROM PUBLIC.{MAIN_DATA["table_name"]})
                 WHERE TABLE_NAME = '{MAIN_DATA["table_name"]}';
    '''
    
    curr.execute(query)
    
    # UPDATING THE CONFIG TABLE
    
    conn.close()
    print("Done")
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }