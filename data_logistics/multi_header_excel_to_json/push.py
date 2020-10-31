#This script will take the json file that was created in excel_to_json.py and upload it to the appropriate table
#in Snowflake

#Import libraries
import sys
import os

# Handle snowflake connection import issues
user_home = os.path.expanduser('~')
sys.path.append(user_home + r'/scion_scripts/extract_load/connections/') #Need this since snowflake connections is in a different directory
from snowflake_connection import snow_conn # pylint: disable=import-error
import snowflake.connector as sc #Need this for error types during try/except blocks

# Function to prepare and load the json data into snowflake
def load_snow(file_name, sheet, target_database, target_schema):

    #Set up variables based on static data or on the argument values
    output_folder = user_home + r'/scion_scripts/extract_load/json/output/' #Use this for directing the output file.
    output_file = f'{file_name}_{sheet}'
    full_path = f'{output_folder}{output_file}.json'
    full_target = f'{target_database}.{target_schema}.{output_file}'.lower() #Make it lower to ensure consistency when printing results
    staging_table = f'{full_target}_staging'
    file_format = 'excel_to_json'
    file_staging = 'json_staging'

    #Establish connection to snowflake
    conn = snow_conn()

    #Create the target table. Adding a try/except block since it's possible that user input could cause this to fail
    ##The target table should be the filename_sheetname. Users should not be able to set it.
    try:
        conn.cursor().execute(
            f'create table if not exists {full_target} '
        '(src variant)'
        )
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: {full_target} not created due to issue with SQL target inputs. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: {full_target} created')
    
    #Create the file format
    try:
        conn.cursor().execute(
            'create or replace file format excel_to_json '
            'type = json strip_outer_array = true'
        )
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: File format {file_format} not created. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: File format {file_format} created.')

    #Create the staging environment
    try:
        conn.cursor().execute(
            f'create or replace temporary stage {file_staging} '
            'file_format = excel_to_json'
        )
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: {file_staging} not created. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCES: {file_staging} created.')

    #Create the temp table to copy the staging data into
    try:
        conn.cursor().execute(
            f'create or replace temporary table {staging_table} '
            '(src variant)'
        )
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: {staging_table} not created. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: {staging_table} created.')

    #Upload the file to the staging environment
    try:
        conn.cursor().execute(f'put file://{full_path} @{file_staging}')
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: {output_file} not uploaded to {file_staging} stage. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: {output_file} uploaded to {file_staging} stage.')
    
    #Copy the data into the temp table
    try:
        conn.cursor().execute(f'copy into {staging_table} from @{file_staging}/{output_file}.json.gz')
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: Data not copied into {staging_table}. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: Data copied into {staging_table}.')

    #Insert the staging data into the production table
    try:
        conn.cursor().execute(
            f'insert into {full_target} '
            f'select src from {staging_table}'
        )
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: Data not inserted into {full_target}. Message: {e}')
    else:
        print(f'SUCCESS: Data inserted into {full_target}.')

    #Remove all files from the staging environment
    try:
        conn.cursor().execute(f'remove @{file_staging}')
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: Files not removed from {file_staging}. Message: {e}')
    else:
        print(f'SUCCESS: Files removed from {file_staging}.')

    #Drop the staging temp table
    try:
        conn.cursor().execute(f'drop table if exists {staging_table}')
    except (sc.errors.ProgrammingError) as e:
        print(f'ERROR: {staging_table} was not dropped. Message: {e}')
    else:
        print(f'SUCCESS: {staging_table} was dropped.')

