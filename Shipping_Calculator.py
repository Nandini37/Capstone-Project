import requests
import json
import math
import streamlit as st
import math
import re
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

def data_generation_pyodbc(sql_command):
    try:
        # Create a connection using pyodbc
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=localhost;'
            'DATABASE=master;'
            'Trusted_Connection=yes;'
        )

        # Use the connection to fetch data into a DataFrame
        df_orders = pd.read_sql(sql_command, conn)
        return df_orders

    except pyodbc.Error as e:
        print(f"Error occurred while connecting to SQL Server: {e}")
        return None



###############################################################
#### Request to Get API Response ###############################

def get_zone_chart(origin_zip, shipping_date = '2024-09-15'):
    # Construct the URL with the given ZIP code and shipping date
    url = f"https://postcalc.usps.com/DomesticZoneChart/GetZoneChart?zipCode3Digit={origin_zip}&shippingDate={shipping_date}"

    # Optional headers (these can be customized if necessary)
    headers = {
        'authority': 'postcalc.usps.com',
        'method': 'GET',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    # Send the GET request to the USPS API
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError if the HTTP request failed
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except Exception as err:
        print(f"Other error occurred: {err}")
        return None
    
    # Parse the JSON data if the request was successful
    try:
        data = response.json()
        return data
    except json.JSONDecodeError:
        print("Failed to parse JSON response.")
        return None

############# Process JSON Response Data ###############################

def process_response_data(response_data):
    # Extract the relevant parts of the JSON data
    columns_data0 = response_data.get('Column0', [])
    columns_data1 = response_data.get('Column1', [])
    columns_data2 = response_data.get('Column2', [])
    
    # Convert the extracted data to DataFrames
    columns_data0_df = pd.DataFrame(columns_data0)
    columns_data1_df = pd.DataFrame(columns_data1)
    columns_data2_df = pd.DataFrame(columns_data2)

    # Concatenate the DataFrames along the rows
    combined_df = pd.concat([columns_data0_df, columns_data1_df, columns_data2_df], axis=0)

    # Return a copy of the combined DataFrame
    return combined_df.copy()

################### Expand ZIP Codes ###############################

# Function to expand ZIP code ranges
def expand_zipcodes(zip_code):
    if '---' in zip_code:
        start, end = zip_code.split('---')
        return [str(i).zfill(3) for i in range(int(start), int(end) + 1)]
    else:
        return [zip_code]

# Function to expand ZIP codes based on the existing DataFrame
def expand_zipcodes_in_df(df):
    expanded_zipcodes = []
    for _, row in df.iterrows():
        zip_codes = expand_zipcodes(row['ZipCodes'])
        for zip_code in zip_codes:
            expanded_zipcodes.append({'ZipCode': zip_code, 'Zone': row['Zone']})
    
    # Convert expanded_zipcodes back to a DataFrame
    expanded_df = pd.DataFrame(expanded_zipcodes)
    return expanded_df

################### Billable Weight Calculation ###############################

def calculate_billable_weight(length, height, width, weight):
    """
    Calculate the billable weight based on dimensions and actual weight.

    Parameters:
    l (float): Length in inches
    b (float): Width in inches
    h (float): Height in inches
    d (float): Actual weight in ounces

    Returns:
    int: Billable weight in ounces
    """
    # Ensure all dimensions and weight are in numeric form
    length = float(length)
    width = float(width)
    height = float(height)
    weight = float(weight)

    # Calculate dimensional weight in ounces
    Dimweight = ((length * width * height) / 166) * 16

    # Calculate billable weight
    Billableweight = max(math.ceil(Dimweight), math.ceil(weight))
    return Billableweight

################### Example SQL Query ###############################

# Example SQL query
#sql_command = """
#                SELECT * FROM ratecard
#              """

# Call the function
#ratecard = data_generation_pyodbc(sql_command)



def get_value(ratecard, zone,billableweight, shipping_days='Ground'):
    # Ensure WeightStartOz and WeightEndOz are numeric (convert if needed)
    ratecard['WeightStartOz'] = pd.to_numeric(ratecard['WeightStartOz'], errors='coerce')
    ratecard['WeightEndOz'] = pd.to_numeric(ratecard['WeightEndOz'], errors='coerce')
    # Filter the ratecard DataFrame based on the given zone and billableweight conditions
    filtered_df = ratecard[
        (ratecard['Zonemapping'] == zone)  
        &(ratecard['WeightStartOz'] < billableweight)  
        &(ratecard['WeightEndOz'] >= billableweight)
        &(ratecard['Shipoption'] == shipping_days)
    ]
    
    # If a matching row is found, return the 'Value' from the filtered DataFrame
    if not filtered_df.empty:
        return filtered_df['Value'].min()
    
    else:
        return None  # Return None if no matching row is found
    
    
def get_othervalue(ratecard,zone,billableweight, shipping_days='Ground'):
    # Ensure WeightStartOz and WeightEndOz are numeric (convert if needed)
    ratecard['WeightStartOz'] = pd.to_numeric(ratecard['WeightStartOz'], errors='coerce')
    ratecard['WeightEndOz'] = pd.to_numeric(ratecard['WeightEndOz'], errors='coerce')
    # Filter the ratecard DataFrame based on the given zone and billableweight conditions
    filtered_df = ratecard[
        (ratecard['Zonemapping'] == zone)  
        &(ratecard['WeightStartOz'] < billableweight)  
        &(ratecard['WeightEndOz'] >= billableweight)
        &(ratecard['Shipoption'] == shipping_days)
    ]
    
    # If a matching row is found, return the 'Value' from the filtered DataFrame
    if not filtered_df.empty:
        return filtered_df
    
    else:
        return None  # Return None if no matching row is found



# Create a new DataFrame with expanded ZIP codes
# Example usage:
#zip_code = "700"  # Example ZIP code
#shipping_date = "8/12/2024"  # Example shipping date in MM/DD/YYYY format
#
#response_data = get_zone_chart(zip_code, shipping_date)
#df = process_response_data(response_data)
#######expanded_df = pd.DataFrame(expanded_zipcodes)

# Usage example:
# expanded_df = expand_zipcodes_in_df(df)
#expanded_df[expanded_df['ZipCode'].astype(str).str[:3] == str(zipcode)[:3]]
#zone = df['Zone'].iloc[0]



# Example usage
# b = 15  # width in inches
# h = 10  # height in inches
# l = 20  # length in inches
# d = 25  # actual weight in ounces

#billable_weight = calculate_billable_weight(l, b, h, d)

def calculate_shipping_cost(length, height, width, weight,origin_zip,destination_zip, shipping_days='Ground'):
    
    billableweight =  calculate_billable_weight(length, height, width, weight)
    response_data = get_zone_chart(origin_zip, shipping_date)  
    df = process_response_data(response_data)
    expanded_df = expand_zipcodes_in_df(df)
    expanded_df['Zone'] = expanded_df['Zone'].astype(str).apply(lambda x: re.sub(r'[^0-9]', '', x))
    expanded_df = expanded_df[expanded_df['ZipCode'].astype(str).str[:3] == str(destination_zip)[:3]]
    zone = int(expanded_df['Zone'].iloc[0])
    sql_command = """
        SELECT *,cast(substring(Zone,5,len(Zone)) as int) as Zonemapping FROM ratecard WHERE cast(substring(Zone,5,len(Zone)) as int) = {}
    """.format(zone)

    # Call the function
    ratecard = data_generation_pyodbc(sql_command)
    rate = get_value(ratecard,zone,billableweight,shipping_days)
    table = get_othervalue(ratecard,zone,billableweight,shipping_days)

    cost = float(rate)
    return cost,billableweight,table
 
 










 
# Streamlit app layout

import streamlit as st 
st.title("Shipping Cost Calculator")

# Input fields
origin_zip = st.text_input("Origin Zipcode")
origin_zip  = str(origin_zip)[:3]
destination_zip = st.text_input("Destination Zipcode")
destination_zip  = str(destination_zip)[:3]
shipping_date = '10/14/2024'
length =  st.number_input("Length (inches)", min_value=0.0)
height =  st.number_input("Height (inches)", min_value=0.0)
width =   st.number_input("Width (inches)", min_value=0.0)
weight =  st.number_input("Weight (ounces)", min_value=0.0)

shipping_days = st.selectbox(
"Shipping Days", 
("2day","Ground")
)

# Button to calculate the shipping cost
if st.button("Calculate Shipping Cost"):
    if origin_zip and destination_zip:
        # Calculate both cost and table
        cost,billableweight, table = calculate_shipping_cost(length, height, width, weight, origin_zip,destination_zip, shipping_days)
        
        # Display the cost
        st.success(f"Estimated Shipping Cost: ${cost:.2f} for Billable weight:  ${billableweight:.2f} ")
        
        # Display the table using Streamlit
        if not table.empty:
            st.write("Shipping Rate Details:")
            st.dataframe(table)
        else:
            st.error("No rate details found for the provided parameters.")
    else:
        st.error("Please enter both origin and destination zip codes.")
# Run the Streamlit app
# You can run the app using the command: streamlit run shipping_app.py