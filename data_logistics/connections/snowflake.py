#!/usr/bin/env python3

# This file can be used to establish a connection to Snowflake based on the current user's credentials
import snowflake.connector as sc
import os
import yaml
import re
import sys

def snow_conn():
  # Parse data from the dbt_profiles.yml file so the snowflake connection info can be re-used here
    user_home = os.path.expanduser('~')
    dbt_profiles = user_home + '/.dbt/profiles.yml'
    try:
      with open(dbt_profiles) as profiles:
        profiles_dict = yaml.load(profiles, Loader=yaml.FullLoader)
    except FileNotFoundError as e:
      print(f'File not found. Message: {e}')

  #Extract just the scion > outputs > dev level dictionary since this is where the connection info is stored  
    conn_dict = profiles_dict['scion']['outputs']['dev']

  # Determine if an environment variable is being used or raw text for pw and username in dbt_profiles
    raw_user = conn_dict['user']
    raw_pwd = conn_dict['password']

    env_var_pattern = re.compile(r"{{ env_var\('(.*?)'\) }}") #Matches this pattern: "{{ env_var('variable_name') }}"

  #If an environment variable is used for password and/or username, parse that into the form needed by the os module to check the actual variable
  # Otherwise, using the raw value in dbt profiles.
  # The try/except statement is used here since if an error occurs during the search lookup, it means no useable value exists.
    try:
        regex_user = re.search(env_var_pattern, raw_user).group(1)
        dbt_user = os.getenv(regex_user) #This references the locally stored environmental variable so the actual value is pulled
    except Exception as e:
      dbt_user = raw_user

    try:
        regex_pwd = re.search(env_var_pattern, raw_pwd).group(1)
        dbt_pwd = os.getenv(regex_pwd)
    except Exception as e:
        dbt_pwd = raw_pwd

  #Establish connection to Snowflake
    try:
      conn = sc.connect(
          #user = dbt_user,
          user = dbt_user,
          password = dbt_pwd,
          account = conn_dict['account'],
          warehouse = conn_dict['warehouse'],
          database = conn_dict['database'],
          schema = conn_dict['schema']
    )
    except sc.errors.DatabaseError as e:
        print(f'Could not connect to snowflake. Message: {e}')
    
    return conn

#Used for validating the connection
# conn = snow_conn()
# results = conn.cursor()
# try:
#     results.execute("SELECT current_version()")
#     one_row = results.fetchone()
#     print(one_row[0])
# finally:
#     results.close()
# conn.close()