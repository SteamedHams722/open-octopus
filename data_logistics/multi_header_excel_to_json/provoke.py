#!/usr/bin/env python3

#This script takes user input for the necessary variables and uses the excel_to_json and json_to_snowflake scripts 
#to create a json file from excel and load it into a table in Snowflake.

#Import libraries
from parse import load_json
from push import load_snow
import sys

#Take user input for the file information
file_name = input('File Name without Extension: ')
sheet = input('Sheet Name (case sensitive): ')


#Take header input and make sure it's an integer greater than 0
def ask_header():
    header_string = str(input('Number of Headers (q to exit): ')).lower().strip() #Bring in the input as a string
    try:
        if header_string[:1] in ['q', 'quit']: #Exit if the user enters q or quit
            sys.exit(0)
        elif int(header_string) > 0: #If the header can convert to integer and is greater than zero, return the input
            return int(header_string)
        else:
            print(f'ERROR: Please enter an integer value greater than 0.') #If the integer is not greater than 0, re-run the function
            return ask_header()
    except (ValueError, TypeError): # If the input is not an integer or q, re-run the function.
        print(f'ERROR: Please enter an integer value greater than 0.')
        return ask_header()

# Create the headers variable based on the function results
headers = ask_header()

#Determine if the user wants to upload a table
def ask_load():
    load_table = str(input("Load a Table? (y/n/q): ")).lower().strip()
    if load_table[:1] in ['y','yes']:
        return True
    elif load_table[:1] in ['n','no']:
        return False
    elif load_table[:1] in ['q','quit']:
        sys.exit(0) #Give the user the opportunity to quit the script
    else:
        print('Please enter y, n, or q.')
        return ask_load() #If the answer is invalid, run the function again.

#Create the load variable based on the response
load = ask_load()

#Gather the database inputs if ask_load returns true
if load:
    database = input('Target Database: ')
    schema = input('Target Schema: ')

#Run load_json and load_snow functions based on user inputs
try:
    load_json(file_name, headers, sheet)
except Exception as e:
    print(f'ERROR: There was an issue with the excel_to_json script. Message: {e}')
    sys.exit(0) #Exit the program if the file was not created.
else:
    if load: #If the ask_load function returns True, run the upload function
        try:
            load_snow(file_name, sheet, database, schema)
        except Exception as e:
            print(f'ERROR: There was an issue with the json_to_snowflake script. Message: {e}')
