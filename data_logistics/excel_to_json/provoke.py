#!/usr/bin/env python3

#This script takes user input for the necessary variables and uses the excel_to_json and json_to_snowflake scripts 
#to create a json file from excel and load it into a table in Snowflake.

#Import libraries
from parse import load_json
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

#Run load_json function based on user inputs
try:
    load_json(file_name, headers, sheet)
except Exception as e:
    print(f'ERROR: There was an issue with the excel_to_json script. Message: {e}')
    sys.exit(0) #Exit the program if the file was not created.
