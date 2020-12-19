'''Important: Create a folder called input in the /multi-tool/data_logistics/excel_to_json path
and store the file(s) you need to parse there'''

#Import libraries.
import pandas as pd
import numpy as np
import simplejson
import os
import shutil
from pprint import pprint
from datetime import datetime, timezone
import sys

#Create the folder variables that will be used across the functions
user_home = os.path.expanduser('~')
user_home = user_home.replace(os.sep,'/')
source_folder = user_home + r'/multi-tool/data_logistics/excel_to_json/input/' #Need to create this folder and add the files to it
output_folder = user_home + r'/multi-tool/data_logistics/excel_to_json/output/' #Use this for directing the output file.

def data_prep(file_name, headers, sheet):
    #Create the file path based on the user inputs
    full_path = f'{source_folder}{file_name}.xlsx'

    #Create the list of headers based on the user input by looping through the range
    head_list = []
    try:
        for head in range(0, headers):
            head_list.append(head)
    except (IndexError, NameError, ValueError, TypeError) as e:
        print(f'ERROR: Header index not created due to issue with header inputs. Message: {e}')
        sys.exit(0)

    #Create a pandas dataframe from the excel file and sheet
    try:
        excel_df = pd.read_excel(full_path, header=head_list, sheet_name=sheet) #First sheet is used by default, but that can be specified if necessary.
    except (IndexError, FileNotFoundError, NameError, ValueError) as e:
        print(f'ERROR: Dataframe for {file_name} not created due to issue with file name, sheet, and/or header inputs. Message: {e}')
        sys.exit(0)
    else:
        print(f'SUCCESS: Dataframe for {file_name} created.')
    
    # Convert datetime fields to string since datetime fields can't be encoded into json
    for col in excel_df.select_dtypes([np.datetime64]):
        excel_df[col] = excel_df[col].astype(str)
        
    #Create a basic list of the dictionaries from the dataframe
    dict_list = excel_df.to_dict(orient='records')

    return dict_list

# #Function to convert each dictionary to a nested dictionary based on the number of headers
def nested_dict(d: dict) -> dict:
    result = {}
    for key, value in d.items():
        key = list(key) #Convert from tuple to list so the contents can use string functions
        key = [' '.join(str(x).replace('.',' ').split()).strip().lower().replace(' ','_') for x in key]
        #replace any periods with empty spaces, split based on each separate value and join based on an empty space to handle excessive white space, remove outer white space, convert to lower case, replace remaining spaces with an underscore
        target = result #target and result now point to the same dictionary, but result holds all the values in the dictionary while target is used to add the next value in the loop
        for k in key[:-1]:  # traverse all keys but the last
            target = target.setdefault(k, {})
        target[key[-1]] = value
    return result #returning target would just give the key/value pair for the last item in the for loop. result returns everything in the dictionary.

#Function to add the individual nested dictionaries to a list so they can be converted to json and load to a file
def load_json(file_name, headers, sheet):

    #Set up variables needed for the output file
    output_file = f'{file_name}_{sheet}'
    output_path = f'{output_folder}{output_file}.json'

    #Bring in the dict list values
    dict_list = data_prep(file_name, headers, sheet)

    #Remove the existing directory if it exists and create the new one. This will keep the output folder clean
    try:
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder) # delete the directory if it exists
        os.makedirs(output_folder) #Recreate the directory
    except OSError as e: #Print any system error that may come up
        print(f'ERROR: {output_folder} not created. Message: {e}') 
        sys.exit(0)
    else:
        print(f'SUCCESS: {output_folder} created.')
    
    # Separate each dictionary from the dict_list, run it against the nesting function, and append it to a new list
    nested_list = []
    try:
        if headers == 1: #If only one header, there's no need to create a nested dictionary
            for dict in dict_list:
                new_dict = {' '.join(str(key).replace('.',' ').split()).strip().lower().replace(' ','_'): val for (key, val) in dict.items()}
                nested_list.append(new_dict)
        else:
            for dict in dict_list: #Separate each dictionary from the list
                nested_list.append(nested_dict(dict)) #Add each nested dictionary back to the list
    except AttributeError as e: #If the header inputs are wrong and contain a number, this will catch it
        print(f'ERROR: {output_file} not created due to issue with nested_dict function on excel_to_json. Message: {e}')
        sys.exit(0)
    
    #Add data as a top level key to help with parsing during the SQL transform phase
    json_list = []
    for dict in nested_list:
       json_list.append({'data':dict})
    
    #Create metadata variables to add to each dictionary in the json_list
    metadata_file = f'{file_name}.xlsx'
    metadata_sheet = sheet
    metadata_now = datetime.now(tz=timezone.utc) #Need to store the current datetime in UTC time
    metadata_timestamp = metadata_now.strftime('%Y-%m-%d %H:%M:%S')
    metadata_dict = {'metadata_timestamp': metadata_timestamp, 'metadata_file': metadata_file, 'metadata_sheet': metadata_sheet}
    
    #Add the metadata fields to each nested dictionary in the json_list
    for dict in json_list:
        dict.update(metadata_dict)

    #Write to the json file. Simple json creates a nicer output and replaces NaN values with null
    # Adding a try block in case the output folder doesn't exist.
    try:
        with open(output_path, 'w') as json_output:
            simplejson.dump(json_list, json_output, indent=2, ignore_nan=True) #ignore_nan sets nan to null
    except FileNotFoundError as e:
        print(f'ERROR: {output_file} not created due to issue writing to json. Message: {e}')
    else:
        print(f'SUCCESS: {output_file}.json created in {output_folder}')