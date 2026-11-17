# This file calls to RTE API and stores the data in MongoDB. It was originally created to schedule data retrieval, but in practice the 
# data retrieval is done manually. The data is stored in a MongoDB database and can be accessed from there.

# Get imports
import requests
import base64
import json
import datetime
import time
from getpass import getpass
from calendar import monthrange
from urllib.parse import quote_plus
import pymongo
import pandas as pd
from pymongo.errors import PyMongoError


# --------------------------------------------------------------------------------------- #

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


def mongo_store_data(data, database_name, collection_name, mongodb_uri):
    """
    Store `data` in MongoDB and print (or return) whether it succeeded.
    """
    # Connect to the MongoDB database
    client = pymongo.MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    try:
        result = collection.insert_one(data)
    except PyMongoError as e:
        # Something went wrong with the insert
        print(f"[ERROR] Failed to store data: {e}")
        client.close()
        return False

    client.close()

    if result.acknowledged:
        print(f"[OK] Stored document with _id={result.inserted_id}")
        return True
    else:
        print("[WARN] MongoDB did not acknowledge the insert")
        return False

# --------------------------------------------------------------------------------------- #

# This file will simply connect to the rte and get the data directly from there

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
    print(response)
    # When positive response, the token is retrieved
    data = response.json()
    oauth = data['access_token']
    
    return(oauth)

# --------------------------------------------------------------------------------------- #

# This function does severall calls to the RTE API (because maximum time between start_date and end_date is 1 month) 
# the argument past_photo is a boolean (True, False) that indicates if we want to make a photo from the past or not
# However, the past_photo part and past_date is not yet implemented.
def get_unavailabilities(oauth, start_date, end_date, mongodb_uri):
    # This should be changed in the case of getting a past_photo because many of the rows that are relevant for that 
    # past photo will not be ACTIVE anymore.
    # unav_status = ['ACTIVE', 'INACTIVE']
    # This could also be changed. Currently it means that if we call the API with start_date=01/01/2023 and end_date=01/02/2023,
    # it will return all the records of unavailabilities that have been updated between the two dates.
    # date_type APPLICATION_DATE gets all unavailabilities with predictions in the defined dates, so that 
    # we can get an unavailability that has updated_date outside the defined dates for start_date and end_date

    # If “date_type” is set to PUBLICATION_DATE and the “start_date” and “end_date” fields are filled in, 
    # the service returns only the versions of unavailabilities published during this period.
    # date_type = 'UPDATED_DATE'
    # date_type = 'PUBLICATION_DATE'
    date_type = 'EVENT_DATE'

    event_status = 'ACTIVE'
    
    # Current year/month/day/hour/minute/second is calculated for the last call to the API. 
    # For instance, if today is 05/05/2023, the last call of the API will be from 01/05/2023 to 05/05/2023 (+current hour,minute,second).
    
    # current_datetime = datetime.datetime.now()
    # current_datetime = datetime.datetime.now(datetime.timezone.utc)

    # previous_datetime = current_datetime - datetime.timedelta(days=20)
    current_datetime = datetime.datetime.now()
    # current_datetime = current_datetime - datetime.timedelta(days=10)
    # previous_datetime = current_datetime - datetime.timedelta(days=10)

    # Set start_date to the beginning of the previous day
    # start_date = previous_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    start_date_formatted = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Set end_date to the end of the previous day
    # end_date = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date_formatted = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
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
    # api_url = f'https://digital.iservices.rte-france.com/open_api/unavailability_additional_information/v6/generation_unavailabilities?event_status={event_status}&date_type={date_type}&start_date={start_date_formatted}&end_date={end_date_formatted}'
    api_url = f'https://digital.iservices.rte-france.com/open_api/unavailability_additional_information/v7/generation_unavailabilities?date_type={date_type}&start_date={start_date_formatted}&end_date={end_date_formatted}'

    print("Running request: ", api_url)
    response = requests.get(api_url, headers=headers, timeout=60)
    response.raise_for_status()

    print("Status code:", response.status_code)
    print("Response text:", response.text)
    print("Content-Type:", response.headers.get("Content-Type"))

    json_response = response.json()
    responses["results"].append(json_response)
    print("Response received:\n", responses)
    # -----------------------------------NEED TO GET THE MONGO STUFF UP------------------------------------------ #

    # Store the responses in MongoDB
    database_name = "data"
    collection_name = "unavs_update"
    mongo_store_data(responses, database_name, collection_name, mongodb_uri)
    # print(f"Data stored in database {database_name} in collection {collection_name}")

    return

# Function to call get_unavailabilities and store the data
def generate_weekly_periods(start_year, end_year, rte_credentials, mongodb_uri):
    
    # Define the start and end dates
    start_date = datetime.datetime(start_year, 7, 1, 0, 0)
    end_date = datetime.datetime(end_year, 7, 15, 0, 0)
    
    # Loop through each 7-day period
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + datetime.timedelta(days=10)  # End of the 7-day period
        if current_end > end_date:
            current_end = end_date  # Adjust the last period to not exceed the end_date
        oauth = get_oauth(rte_credentials)
        actual_gen = get_unavailabilities(
            oauth, current_start, current_end, mongodb_uri
        )
        print(current_start, current_end, actual_gen)
        current_start = current_start + datetime.timedelta(days=9)  # Move to the next 7-day period

    print("Done!")

def main():
    print("RTE unavailability data to MongoDB")
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

# # Schedule the function to run at 00:05
# schedule.every().day.at("15:55").do(call_and_store)  # Adjust the time as needed

# if __name__ == '__main__':
#     # Run the scheduling loop
#     while True:
#         # schedule.run_pending()
#         # Get the current date and time
#         current_time = datetime.datetime.now().time()

#         # Extract the hour and minute components
#         hour = current_time.hour
#         minute = current_time.minute
#         # Combine hour and minute into a single variable as a string
#         second = current_time.second
        
#         # Print the current time in hh:mm format
#         # print(f"The current time is: {hour:02d}:{minute:02d}:{second:02d}")
        

#         # Combine hour, minute, and millisecond into a single variable as a string
#         time_combined = f"{hour:02d}:{minute:02d}:{second:02d}"
#         scheduled_time ="15:55:10"
#         if time_combined == scheduled_time:
#             print("Beginning function:")
#             call_and_store()
#         else:
#             continue
        
#         time.sleep(1)
