import psycopg2, os, datetime,time,csv,tempfile
from datetime import datetime,date
from dotenv import dotenv_values, load_dotenv
import snowflake.snowpark.functions
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType,ArrayType
import pandas as pd

# load_dotenv()

snowflake_secret = dotenv_values(".sf.mahesh")

SNOWFLAKE_USERNAME=snowflake_secret.get('SNOWFLAKE_USERNAME')
SNOWFLAKE_PASSWORD=snowflake_secret.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT=snowflake_secret.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE=snowflake_secret.get('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE=snowflake_secret.get('SNOWFLAKE_DATABASE')

def log_error(session,e):
    print("Error Msg: ",e)
    return e

def write_data_to_table(conn_pg,source_table,target_table,target_stage):
    print('load to snowflake stage started')
    print(source_table)

    try:
        # batch_size=100000
        cur=conn_pg.cursor()
        cur.execute('select * from {};'.format(source_table))
        colnames = [desc[0] for desc in cur.description]
        print(colnames)

        ###### fetch data from cursor and load into snowflake stage table
        while True:
            # rows = cur.fetchmany(batch_size)
            rows = cur.fetchall()
            if not rows:
                break
            data = [tuple(i) for i in rows]
            df = session.create_dataframe(data,schema=colnames)

            cur_date = date.today().strftime("%Y%m%d")
            cur_ts = datetime.now().strftime("%H%M%S")
            df.write.copy_into_location("@{}_{}_{}.csv".format(target_stage,cur_date,cur_ts),file_format_name="SAHIL_CXAPP.CXAPP_PROD_KIPI.POSTGRES_TO_SNOWFLAKE_UNLOAD_FF", overwrite=True, single=True,header=True)
            # df.write.mode('overwrite').save_as_table("{}".format(target_table))

        # rows = cur.fetchall()
        # data = [tuple(i) for i in rows]
        # df = session.create_dataframe(rows,schema=colnames)
        # df.write.copy_into_location("@{}_{}_{}.csv".format(target_stage,date.today().strftime("%Y%m%d"),datetime.now().strftime("%H%M%S")),file_format_name="SAHIL_CXAPP.CXAPP_PROD_KIPI.POSTGRES_TO_SNOWFLAKE_UNLOAD_FF", overwrite=True, single=True,header=True)

        print('load to snowflake stage ended')
        cur.close()
    
    except Exception as e:
        log_error(session,e)

def connect(combined_details,company_id):
    ###### Fetching postgres details to make connection
    PG_COMPANY_NAME = combined_details.select("company_name").filter(col("company_id") == company_id).collect()[0][0]
    PG_HOSTNAME = combined_details.select("db_host").filter(col("company_id") == company_id).collect()[0][0]
    PG_PORT = combined_details.select("db_port").filter(col("company_id") == company_id).collect()[0][0]
    PG_USERNAME = combined_details.select("db_username").filter(col("company_id") == company_id).collect()[0][0]
    PG_PASSWORD = combined_details.select("db_password").filter(col("company_id") == company_id).collect()[0][0]
    PG_DATABASE = combined_details.select("database_name").filter(col("company_id") == company_id).collect()[0][0]
    PG_TABLE_NAMES_STR = combined_details.select("table_name").filter(col("company_id") == company_id).distinct().collect()
    PG_TABLE_NAMES = [PG_TABLE_NAMES_STR[i][0] for i in range(len(PG_TABLE_NAMES_STR))]

    print(PG_COMPANY_NAME,PG_TABLE_NAMES)

    ###### Postgres connection
    conn_pg = psycopg2.connect(
    host=PG_HOSTNAME, port=PG_PORT, user=PG_USERNAME, password=PG_PASSWORD, database=PG_DATABASE
    )

    ###### Calling function to fetch data from postgres and write to table
    for j in PG_TABLE_NAMES:
        target_table = '{}.CXAPP_PROD_{}.RAW_EVENT_{}'.format(SNOWFLAKE_DATABASE,PG_COMPANY_NAME.upper(),j)
        target_stage = '{}.CXAPP_PROD_{}.table_details/{}/{}'.format(SNOWFLAKE_DATABASE,PG_COMPANY_NAME.upper(),j,j)
        write_data_to_table(conn_pg,j,target_table,target_stage)

    conn_pg.close()

if __name__ == '__main__':
    try:
        print('start time ', datetime.now())

        sf_conn_parameter = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USERNAME,
        "password": SNOWFLAKE_PASSWORD,
        "role": "ACCOUNTADMIN",
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": "COMMON",
        }

        session = Session.builder.configs(sf_conn_parameter).create()

        ###### Fetch company names and credentials details
        company = session.table("{}.COMMON.COMPANY_DETAILS".format(SNOWFLAKE_DATABASE))
        company_credentials = session.table("{}.COMMON.COMPANY_CREDENTIALS".format(SNOWFLAKE_DATABASE))
        company_table_details = session.table("{}.COMMON.POSTGRES_TABLES_TO_EXTRACT".format(SNOWFLAKE_DATABASE))

        ###### Get each company's credentials 
        company_wise_details_df = company.join(company_credentials,company['COMPANY_ID']==company_credentials['COMPANY_ID'],'inner')
        company_wise_details_df_select = company_wise_details_df.select((company["COMPANY_ID"]).alias("company_id"),"company_name",col("is_active").as_("company_is_active"),"cloud_provider","region","is_schema_created","db_host","db_port","db_username","db_password","database_name")
        
        ###### Get company's table names
        combined_details_df = company_wise_details_df_select.join(company_table_details,company_table_details['COMPANY_ID'] == company_wise_details_df_select['COMPANY_ID'],'inner')
        combined_details = combined_details_df.select((company["COMPANY_ID"]).alias("company_id"),"company_name","company_is_active","cloud_provider","region","is_schema_created","db_host","db_port","db_username","db_password","database_name","table_name","extraction_type","is_active")
        # combined_details.show()

        ###### Fetch only the unique company ids
        unique_company_id = combined_details.select("company_id").distinct().collect()
        
        ###### Loop through each company id and create Postgres connection to load tables for each company
        for i in range(len(unique_company_id)):
            print(unique_company_id[i][0])
            connect(combined_details,unique_company_id[i][0])

        session.close()
        print('end time ', datetime.now())
    
    except Exception as e:
        log_error(session,e)