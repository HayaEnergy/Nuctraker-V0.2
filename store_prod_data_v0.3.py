# Get imports
import requests
import base64
import json
import datetime
from getpass import getpass
from urllib.parse import quote_plus
# import schedule
import time
from calendar import monthrange
import pymongo  
import pandas as pd
from pymongo.errors import PyMongoError


def prompt_required(label, secret=False):
    """Prompt until the user supplies a non-empty value."""
    while True:
        value = getpass(f"{label}: ") if secret else input(f"{label}: ")
        value = value.strip()
        if value:
            return value
        print(f"{label} cannot be empty.")


def prompt_year(label):
    """Request a four-digit year from the terminal."""
    while True:
        value = prompt_required(label)
        if value.isdigit() and len(value) == 4:
            return int(value)
        print("Enter a four-digit year, for example 2026.")


def build_mongodb_uri(user, password, host):
    """Build a MongoDB Atlas URI from terminal inputs."""
    clean_host = host.strip()
    clean_host = clean_host.removeprefix("mongodb+srv://")
    clean_host = clean_host.removeprefix("mongodb://")
    clean_host = clean_host.split("/", 1)[0].strip()
    if not clean_host:
        raise ValueError("MongoDB host cannot be empty.")
    return (
        f"mongodb+srv://{quote_plus(user)}:{quote_plus(password)}@{clean_host}/"
        "?retryWrites=true&w=majority&connectTimeoutMS=50000"
    )


def get_prod_nuc(dictonary):
    # Function cleans data to get only nuclear to store in mongodb
    # Flatten the data
    actual_gen = dictonary

    flat_data = []

    for result in actual_gen['results']:
        for generation in result['actual_generations_per_unit']:
            for value in generation['values']:
                entry = {
                    'unit_eic_code': generation['unit']['eic_code'],
                    'unit_name': generation['unit']['name'],
                    'unit_production_type': generation['unit']['production_type'],
                    'generation_start_date': generation['start_date'],
                    'generation_end_date': generation['end_date'],
                    'value_start_date': value['start_date'],
                    'value_end_date': value['end_date'],
                    'value_updated_date': value['updated_date'],
                    'value': value['value']
                }
                flat_data.append(entry)

    # Convert to DataFrame
    df = pd.DataFrame(flat_data)

    nuc_gen = df[df['unit_production_type'] == 'NUCLEAR']

    # Convert value_start_date to datetime
    nuc_gen['value_start_date'] = pd.to_datetime(df['value_start_date'])

    # Pivot the DataFrame
    pivot_df = nuc_gen.pivot(index='value_start_date', columns='unit_name', values='value')

    # Step 5: Transpose the DataFrame
    # transposed_df = pivot_df.T

    # # Step 6: Reset the index to have the plant names as a column
    # transposed_df.reset_index(inplace=True)

    # Rename columns appropriately
    # pivot_df.rename(columns={"index": "Date"}, inplace=True)

    # Step 1: Rename the index to 'Date'
    pivot_df.index.name = 'Date'

    # Step 2: Reset the index so 'Date' becomes a column
    pivot_df.reset_index(inplace=True)  
    print(pivot_df['Date'])

    # Convert 'Date' to datetime 
    pivot_df['Date'] = pd.to_datetime(pivot_df['Date'], utc=True)
    # Remove the timezone information to make the datetimes timezone-naive
    pivot_df['Date'] = pivot_df['Date'].dt.tz_convert(None)

    print(pivot_df['Date'])
    # Extract the date part from 'Date'
    pivot_df['Date'] = pivot_df['Date'].dt.date

    # Group by the date and calculate the mean for each column
    daily_mean_pivot_df = pivot_df.groupby('Date').mean().reset_index()

    # transposed_df_2 = pivot_df.copy()
    
    # # Step 1: Convert 'Date' column to string and split by 'T' to extract the date part
    # transposed_df_2['Date'] = transposed_df_2['Date'].astype(str).str.split(' ').str[0]

    # # Step 2: Group by 'Date' and calculate the mean for each column
    # df_grouped = transposed_df_2.groupby('Date').mean().reset_index()

    daily_mean_pivot_df['Date'] = daily_mean_pivot_df['Date'].astype(str)

    # Convert DataFrame to JSON
    json_data = daily_mean_pivot_df.to_json(orient='records')
    
    json_list = json.loads(json_data)

    return json_list

# Store normal size data
def mongo_store_data(data, database_name, collection_name, mongodb_uri):
    """
    This function stores the output of the nucmonitor in the database. It takes the data to be stored, the database name it will be stored
    in and the collection name. These are defined in the gui.
    For now, the database is not GridFS. This may change in future,
    since the function as it stands cannot store anything larger than 16MB. The data is stored as a JSON.
    """
    
    # Connect to the MongoDB database
    client = pymongo.MongoClient(mongodb_uri)

    db = client[database_name]
    collection = db[collection_name]

    # Insert the data into the collection
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    # Close the database connection
    client.close()

# Function to create an authentication token. This token is then used in the HTTP requests to the API for authentication.
# It is necessary to receive data from RTE.
def get_oauth(rte_credentials):
    # ID from the user. This is encoded to base64 and sent in an HTTP request to receive the oauth token.
    # This ID is from my account (RMP). However, another account can be created in the RTE API portal and get another ID.
    b64_ID = base64.b64encode(rte_credentials.encode('utf-8'))
    b64_ID_decoded = b64_ID.decode('utf-8')
    
    # Headers for the HTTP request
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Authorization': f'Basic {b64_ID_decoded}'}
    api_url = 'https://digital.iservices.rte-france.com/token/oauth/'
    # Call to the API and if successful, the response will be 200.
    response = requests.post(api_url, headers=headers, timeout=60)
    response.raise_for_status()
    
    # When positive response, the token is retrieved
    data = response.json()
    oauth = data['access_token']
    
    return(oauth)

# --------------------------------------------------------------------------------------- #

# This function does severall calls to the RTE API (because maximum time between start_date and end_date is 1 month) 
# the argument past_photo is a boolean (True, False) that indicates if we want to make a photo from the past or not
# However, the past_photo part and past_date is not yet implemented.
def get_production(oauth, start_date_input, end_date_input, mongodb_uri):
    # This should be changed in the case of getting a past_photo because many of the rows that are relevant for that 
    # past photo will not be ACTIVE anymore.
    # unav_status = ['ACTIVE', 'INACTIVE']
    # This could also be changed. Currently it means that if we call the API with start_date=01/01/2023 and end_date=01/02/2023,
    # it will return all the records of unavailabilities that have been updated between the two dates.
    # date_type APPLICATION_DATE gets all unavailabilities with predictions in the defined dates, so that 
    # we can get an unavailability that has updated_date outside the defined dates for start_date and end_date
    
    # Current year/month/day/hour/minute/second is calculated for the last call to the API. 
    # For instance, if today is 05/05/2023, the last call of the API will be from 01/05/2023 to 05/05/2023 (+current hour,minute,second).
    
    current_datetime = datetime.datetime.now()
    # previous_datetime = current_datetime - datetime.timedelta(days=7)
    # current_datetime = datetime.datetime.now()
    # current_datetime = current_datetime - datetime.timedelta(days=49)
    # previous_datetime = current_datetime - datetime.timedelta(days=20)

    # Set start_date to the beginning of the previous day
    start_date = start_date_input
    start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%S%z')

    # Set end_date to the end of the previous day
    end_date = end_date_input
    end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%S%z')
    
    # Headers for the HTTP request
    headers = {'Host': 'digital.iservices.rte-france.com',
               'Authorization': f'Bearer {oauth}'
        }
    
    # the responses object is where we are going to store all the responses from the API.
    # Initially, current_datetime is included to know when we have called the API and all the
    # individual results of the API (because each call is Maz 1 month) are stored in responses["results"]
    responses = {"current_datetime": current_datetime.strftime("%m/%d/%Y, %H:%M:%S"),
                 "results":[]
        }
                
    # Call to the API
    # api_url = f'https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_production_type?start_date={start_date_formatted}%2B02:00&end_date={end_date_formatted}%2B02:00'
    # Use this to get individual generation
    api_url = f'https://digital.iservices.rte-france.com/open_api/actual_generation/v1/actual_generations_per_unit?start_date={start_date_formatted}%2B02:00&end_date={end_date_formatted}%2B02:00'

    print("Running request: ", api_url)
    response = requests.get(api_url, headers=headers, timeout=60)
    response.raise_for_status()
    json_response = response.json()
    responses["results"].append(json_response)
    print("Response received:\n", responses)
    # -----------------------------------NEED TO GET THE MONGO STUFF UP------------------------------------------ #
    # Get only nuclear to not overload with data
    nuc_responses = get_prod_nuc(responses)

    # Will only store nuclear responses since it is hourly and we don't have infinite storage
    # Store the responses in MongoDB

    database_name = "data"
    # collection_name = "prod_update_nuc_avg"
    collection_name = "nuclear_production"

    mongo_store_data(
        nuc_responses, database_name, collection_name, mongodb_uri
    )
    print(f"Data stored in database {database_name} in collection {collection_name}")

    return nuc_responses


def generate_weekly_periods(start_year, end_year, rte_credentials, mongodb_uri):
    
    # Define the start and end dates
    start_date = datetime.datetime(start_year, 7, 1, 0, 0)
    end_date = datetime.datetime(end_year, 7, 15, 0, 0)
    
    # Loop through each 7-day period
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + datetime.timedelta(days=6)  # End of the 7-day period
        if current_end > end_date:
            current_end = end_date  # Adjust the last period to not exceed the end_date
        oauth = get_oauth(rte_credentials)
        actual_gen = get_production(
            oauth, current_start, current_end, mongodb_uri
        )
        print(current_start, current_end, actual_gen)
        current_start = current_start + datetime.timedelta(days=5)  # Move to the next 7-day period

    print("Done!")


# def generate_weekly_periods(start_year, end_year):
#     compiled_nuc = pd.DataFrame()

#     start_date = datetime.datetime(start_year, 12, 16, 0, 0)
#     end_date = datetime.datetime(end_year, 1, 14, 0, 0)

#     current_start = start_date
#     while current_start < end_date:
#         current_end = current_start + datetime.timedelta(days=2)
#         if current_end > end_date:
#             current_end = end_date

#         try:
#             oauth = get_oauth()
#             actual_gen = get_production(oauth, current_start, current_end)

#             # Ensure concat always works
#             if actual_gen is None or not isinstance(actual_gen, pd.DataFrame):
#                 actual_gen = pd.DataFrame()

#             compiled_gas = pd.concat([compiled_gas, actual_gen], ignore_index=True)

#             print(current_start, current_end, f"rows={len(actual_gen)}")

#         except Exception as e:
#             # Catch anything unexpected so we keep looping
#             print(f"Skipping period {current_start} -> {current_end} due to error: {e}")

#         current_start = current_start + datetime.timedelta(days=1)

#     # start_date_str = start_date.strftime('%Y-%m-%d')
#     # end_date_str   = end_date.strftime('%Y-%m-%d')

#     # Always export
#     # out_name = f"nuclear_production_{start_date_str}_to_{end_date_str}.xlsx"
#     # compiled_nuc.to_excel(out_name, index=False)
#     print(f"Done!")

#     # return compiled_nuc, start_date_str, end_date_str


def main():
    print("RTE nuclear production data to MongoDB")
    print("Enter the connection details requested below.")

    mongo_user = prompt_required("MongoDB username")
    mongo_password = prompt_required("MongoDB password", secret=True)
    mongo_host = prompt_required(
        "MongoDB host (for example cluster.example.mongodb.net)"
    )
    rte_client_id = prompt_required("RTE client ID")
    rte_client_secret = prompt_required("RTE client secret", secret=True)
    start_year = prompt_year("Start year (YYYY)")
    end_year = prompt_year("End year (YYYY)")

    if end_year < start_year:
        raise ValueError("End year must be greater than or equal to start year.")

    mongodb_uri = build_mongodb_uri(
        mongo_user, mongo_password, mongo_host
    )
    rte_credentials = f"{rte_client_id}:{rte_client_secret}"
    generate_weekly_periods(
        start_year, end_year, rte_credentials, mongodb_uri
    )


if __name__ == "__main__":
    try:
        main()
    except (ValueError, requests.RequestException, PyMongoError) as exc:
        print(f"[ERROR] {exc}")

# Define the start and end dates
# start_date = datetime.datetime(2024, 1, 1, 0, 0)
# end_date = datetime.datetime(2024, 9, 5, 0, 0)

# Loop through each 7-day period
# current_start = start_date
# while current_start < end_date:
#     current_end = current_start + datetime.timedelta(days=1)  # End of the 7-day period
#     if current_end > end_date:
#         current_end = end_date  # Adjust the last period to not exceed the end_date

# oauth = get_oauth()
# actual_gen = get_production(oauth, start_date, end_date)
