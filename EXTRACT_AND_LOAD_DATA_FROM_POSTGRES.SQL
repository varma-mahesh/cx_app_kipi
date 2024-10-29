CREATE OR REPLACE PROCEDURE UTILS.EXTRACT_AND_LOAD_DATA_FROM_POSTGRES("company_name" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'psycopg2')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (external_database_network_rule_ext_int)
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
import psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def log_message(message):
    """Helper function to log messages."""
    print(message)

def extract_and_stage_data(session, connection_info, table_info):
    # Initialize variables
    table_name = None
    company_id = None
    try:
        company_id = table_info['COMPANY_ID']
        company_name = session.sql(f"SELECT company_name FROM utils.company_details WHERE company_id = '{company_id}'").collect()[0][0]
        table_name = table_info['TABLE_NAME']
        extraction_type = table_info['EXTRACTION_TYPE']
        incremental_column = table_info['INCREMENTAL_COLUMN']
        last_extracted_timestamp = table_info['LAST_EXTRACTED_TIMESTAMP']
        
        # Prepare PostgreSQL query
        where_clause = ""
        if extraction_type == 'incremental' and incremental_column and last_extracted_timestamp:
            where_clause = f"WHERE {incremental_column} >= '{last_extracted_timestamp}' AND {incremental_column} <= CURRENT_TIMESTAMP"
        query = f"SELECT * FROM public.{table_name} {where_clause};"
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            database=connection_info[4],
            user=connection_info[0],
            password=connection_info[1],
            host=connection_info[2],
            port=connection_info[3]
        )
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            log_message(f"[INFO] No records found for table {table_name}.")
            return f"No records found for table {table_name}."

        # Define parameters for splitting data into chunks
        rows_per_file = 100000
        num_files = len(rows) // rows_per_file + (1 if len(rows) % rows_per_file > 0 else 0)

        # Split data and save to stage
        for file_index in range(num_files):
            start_row = file_index * rows_per_file
            end_row = start_row + rows_per_file
            data_chunk = rows[start_row:end_row]

            # Convert to Snowpark DataFrame
            df = session.create_dataframe(data_chunk, schema=[desc[0].upper() for desc in cursor.description])

            df.save_as_table()

            
            
            

            # Define stage location and save data using Snowpark DataFrame write functionality
            csv_file_name = f"{table_name}_part{file_index+1}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
            stage_location = f"@INPX_CXAPP_{company_name}.{company_name}_stage/{csv_file_name}"
            df.write.copy_into_location(
                stage_location,
                file_format_name="CXAI_DEV.SAHIL.POSTGRES_TO_SNOWFLAKE_UNLOAD_FF",
                overwrite=True,
                header=True,
                single=True
            )
            log_message(f"[SUCCESS] Saved file: {csv_file_name} with {len(data_chunk)} rows to {stage_location}.")

            # Load data from the staged file to the target table using Snowpark's `copy_into_table`
            target_table = f"INPX_CXAPP_{company_name}.RAW_{table_name}"
            session.copy_into_table(
                target_table,
                stage_location,
                file_format="CXAI_DEV.SAHIL.POSTGRES_TO_SNOWFLAKE_UNLOAD_FF",
                pattern=csv_file_name,
                transformation=(
                    'SELECT $1 AS FILE_NAME, $2 AS FILE_ROW_NUMBER, * '
                    'FROM @INPX_CXAPP_{company_name}.{company_name}_stage/{csv_file_name}'
                )
            )
            log_message(f"[SUCCESS] Data loaded into target table: {target_table} from {csv_file_name}")

        # Update last_extracted_timestamp
        session.sql(f"""
            UPDATE utils.postgres_tables_to_extract
            SET last_extracted_timestamp = CURRENT_TIMESTAMP
            WHERE company_id = {company_id} AND table_name = '{table_name}'
        """).collect()

        cursor.close()
        conn.close()

        return f"Data extracted and loaded successfully for table {table_name}."

    except Exception as e:
        return f"[ERROR] Error extracting and loading data for table {table_name if table_name else 'unknown'}: {str(e)}"

def main(session, company_name):
    try:
        # Fetch PostgreSQL connection details
        connection_query = session.sql(f"""
            SELECT db_username, db_password, db_host, db_port, database_name
            FROM utils.company_credentials
            WHERE company_id = (
                SELECT company_id FROM utils.company_details WHERE company_name = '{company_name}'
            )
        """)
        connection_info = connection_query.collect()[0]

        # Fetch active tables for the specified company
        active_tables_query = session.sql(f"""
            SELECT company_id, table_name, extraction_type, incremental_column, last_extracted_timestamp 
            FROM utils.postgres_tables_to_extract 
            WHERE is_active = TRUE AND company_id = (
                SELECT company_id FROM utils.company_details WHERE company_name = '{company_name}'
            )
        """)
        active_tables = active_tables_query.collect()

        if not active_tables:
            return f"No active tables found for company '{company_name}' for extraction."

        # Process tables in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_table = {executor.submit(extract_and_stage_data, session, connection_info, table): table for table in active_tables}
            for future in as_completed(future_to_table):
                result = future.result()
                log_message(result)

        return "Data extraction and loading completed successfully."

    except Exception as e:
        return f"[ERROR] Error occurred: {str(e)}"
$$;
