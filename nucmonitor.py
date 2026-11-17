import pandas as pd
import json
import datetime
import pymongo
from bson import ObjectId
import numpy as np


class NucMonitor:

    def __init__(self, mongodb_uri=None):
        if not mongodb_uri:
            raise ValueError(
                "NucMonitor requires the MongoDB URI created by the main pipeline."
            )
        self.mongodb_uri = mongodb_uri
        self.plants_metadata = {"BELLEVILLE 1": 1310.0, "BELLEVILLE 2": 1310.0, "BLAYAIS 1": 910.0, "BLAYAIS 2": 910.0, 
                    "BLAYAIS 3": 910.0, "BLAYAIS 4": 910.0, "BUGEY 2": 910.0, "BUGEY 3": 910.0, "BUGEY 4": 880.0, 
                    "BUGEY 5": 880.0, "CATTENOM 1": 1300.0, "CATTENOM 2": 1300.0, "CATTENOM 3": 1300.0, 
                    "CATTENOM 4": 1300.0, "CHINON 1": 905.0, "CHINON 2": 905.0, "CHINON 3": 905.0, 
                    "CHINON 4": 905.0, "CHOOZ 1": 1500.0, "CHOOZ 2": 1500.0, "CIVAUX 1": 1495.0, 
                    "CIVAUX 2": 1495.0, "CRUAS 1": 915.0, "CRUAS 2": 915.0, "CRUAS 3": 915.0, "CRUAS 4": 915.0, 
                    "DAMPIERRE 1": 890.0, "DAMPIERRE 2": 890.0, "DAMPIERRE 3": 890.0, "DAMPIERRE 4": 890.0, 
                    "FLAMANVILLE 1": 1330.0, "FLAMANVILLE 2": 1330.0,  "FLAMANVILLE 3": 1620.0, "GOLFECH 1": 1310.0, "GOLFECH 2": 1310.0, 
                    "GRAVELINES 1": 910.0, "GRAVELINES 2": 910.0, "GRAVELINES 3": 910.0, "GRAVELINES 4": 910.0, 
                    "GRAVELINES 5": 910.0, "GRAVELINES 6": 910.0, "NOGENT 1": 1310.0, "NOGENT 2": 1310.0, 
                    "PALUEL 1": 1330.0, "PALUEL 2": 1330.0, "PALUEL 3": 1330.0, "PALUEL 4": 1330.0, "PENLY 1": 1330.0, 
                    "PENLY 2": 1330.0, "ST ALBAN 1": 1335.0, "ST ALBAN 2": 1335.0, "ST LAURENT 1": 915.0, 
                    "ST LAURENT 2": 915.0, "TRICASTIN 1": 915.0, "TRICASTIN 2": 915.0, "TRICASTIN 3": 915.0, 
                    "TRICASTIN 4": 915.0, "FESSENHEIM 1": 880.0, "FESSENHEIM 2": 880.0}  # Your plants metadata dictionary

    def _mongo_client(self):
        """Create a MongoDB client using credentials supplied at runtime."""
        return pymongo.MongoClient(
            self.mongodb_uri,
            serverSelectionTimeoutMS=50_000,
        )

    def add_total(self, data):
        total_values = {}
        for key in data:
            daily_values = data[key]
            total = sum(daily_values.values())
            daily_values["Total"] = total
            for date, value in daily_values.items():
                if date not in total_values:
                    total_values[date] = value
                else:
                    total_values[date] += value

        data["Total"] = total_values

    def mongo_unavs_call(self, user_input_start_date, user_input_end_date, user_input_past_date):
        print("Starting mongo_unavs_call")
        # Use the same runtime connection configured by the main pipeline.
        client = self._mongo_client()

        db = client["data"]
        collection_past_unavs = db["unavs"]
        collection_unavs = db["unavs_update"]

        start_date = pd.Timestamp(user_input_start_date).strftime("%Y-%m-%dT00:00:00")
        end_date = pd.Timestamp(user_input_end_date).strftime("%Y-%m-%dT23:59:59")
        past_date = pd.Timestamp(user_input_past_date).strftime("%Y-%m-%dT23:59:59")


        pipeline_v4 = [
            # 1) Expand each results element into its own doc
            { "$unwind": "$results" },

            # 2) Expand each generation_unavailabilities element
            { "$unwind": "$results.generation_unavailabilities" },

            # 3) Keep only those that match your fuel_type + date criteria
            {
                "$match": {
                    "results.generation_unavailabilities.production_type": "NUCLEAR",
                    "results.generation_unavailabilities.updated_date": { "$lte": past_date },
                    "results.generation_unavailabilities.start_date": { "$lte": end_date },
                    "results.generation_unavailabilities.end_date": { "$gte": start_date }

                }
            },

            # 4) Replace the entire document with just that sub-doc
            {
                "$replaceRoot": {
                    "newRoot": "$results.generation_unavailabilities"
                }
            }
        ]


        pipeline_v6 = [
            # 1) Expand each results element into its own doc
            { "$unwind": "$results" },

            # 2) Expand each generation_unavailabilities element
            { "$unwind": "$results.generation_unavailabilities" },

            # 3) Keep only those that match your fuel_type + date criteria
            {
                "$match": {
                    "results.generation_unavailabilities.fuel_type": "NUCLEAR",
                    "results.generation_unavailabilities.publication_date": { "$lte": past_date },
                    "results.generation_unavailabilities.start_date": { "$lte": end_date },
                    # "results.generation_unavailabilities.start_date": { "$gte": start_date },
                    "results.generation_unavailabilities.end_date": { "$gte": start_date },
                    # "results.generation_unavailabilities.end_date": { "$lte": end_date }
                }
            },

            # 4) Replace the entire document with just that sub-doc
            {
                "$replaceRoot": {
                    "newRoot": "$results.generation_unavailabilities"
                }
            }
        ]

        result1 = list(collection_past_unavs.aggregate(pipeline_v4))
        result2 = list(collection_unavs.aggregate(pipeline_v4))
        result_v6 = list(collection_unavs.aggregate(pipeline_v6))
        merge_results = result1 + result2 + result_v6
        client.close()
        return merge_results

    # --------------------------------------------------------------------------------------- #

    # Convert the dictionary of dictionaries to JSON
    def convert_to_json(self, item):
        if isinstance(item, dict):
            return {str(k): self.convert_to_json(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [self.convert_to_json(i) for i in item]
        elif isinstance(item, ObjectId):
            return str(item)
        else:
            return item
    # --------------------------------------------------------------------------------------- #

    # Function gives the total of the data. When printed as dataframe/excel,
    # Will give a final row with the total for each plant and the total overall
    def add_total(self, data):
        total_values = {}
        for key in data:
            daily_values = data[key]
            total = sum(daily_values.values())
            daily_values["Total"] = total
            for date, value in daily_values.items():
                if date not in total_values:
                    total_values[date] = value
                else:
                    total_values[date] += value
            
        data["Total"] = total_values

    # --------------------------------------------------------------------------------------- #

    def nuc_monitor(self, usr_start_date, usr_end_date, mongo_db_data):
        # # Slightly changed metadata to fit the data from the RTE API: ST-LAURENT B 2 --> ST LAURENT 2, ....

        plants_metadata = {"BELLEVILLE 1": 1310.0, "BELLEVILLE 2": 1310.0, "BLAYAIS 1": 910.0, "BLAYAIS 2": 910.0, 
                        "BLAYAIS 3": 910.0, "BLAYAIS 4": 910.0, "BUGEY 2": 910.0, "BUGEY 3": 910.0, "BUGEY 4": 880.0, 
                        "BUGEY 5": 880.0, "CATTENOM 1": 1300.0, "CATTENOM 2": 1300.0, "CATTENOM 3": 1300.0, 
                        "CATTENOM 4": 1300.0, "CHINON 1": 905.0, "CHINON 2": 905.0, "CHINON 3": 905.0, 
                        "CHINON 4": 905.0, "CHOOZ 1": 1500.0, "CHOOZ 2": 1500.0, "CIVAUX 1": 1495.0, 
                        "CIVAUX 2": 1495.0, "CRUAS 1": 915.0, "CRUAS 2": 915.0, "CRUAS 3": 915.0, "CRUAS 4": 915.0, 
                        "DAMPIERRE 1": 890.0, "DAMPIERRE 2": 890.0, "DAMPIERRE 3": 890.0, "DAMPIERRE 4": 890.0, 
                        "FLAMANVILLE 1": 1330.0, "FLAMANVILLE 2": 1330.0,  "FLAMANVILLE 3": 1620.0, "GOLFECH 1": 1310.0, "GOLFECH 2": 1310.0, 
                        "GRAVELINES 1": 910.0, "GRAVELINES 2": 910.0, "GRAVELINES 3": 910.0, "GRAVELINES 4": 910.0, 
                        "GRAVELINES 5": 910.0, "GRAVELINES 6": 910.0, "NOGENT 1": 1310.0, "NOGENT 2": 1310.0, 
                        "PALUEL 1": 1330.0, "PALUEL 2": 1330.0, "PALUEL 3": 1330.0, "PALUEL 4": 1330.0, "PENLY 1": 1330.0, 
                        "PENLY 2": 1330.0, "ST ALBAN 1": 1335.0, "ST ALBAN 2": 1335.0, "ST LAURENT 1": 915.0, 
                        "ST LAURENT 2": 915.0, "TRICASTIN 1": 915.0, "TRICASTIN 2": 915.0, "TRICASTIN 3": 915.0, 
                        "TRICASTIN 4": 915.0, "FESSENHEIM 1": 0.0, "FESSENHEIM 2": 0.0}

        # --------------------- INITIAL DATA CLEANING FOR MONGO DATA ------------------------ #    

        # # Create a DataFrame
        # mongo_data = mongo_unavs_call(usr_start_date, usr_end_date, past_date)
        # mongo_data = get_mongodb_data(usr_start_date, usr_end_date, past_date)
        # print(mongo_db_data)
        mongo_df = pd.DataFrame(mongo_db_data)

        # the five columns you care about
        _optional = {
            "updated_date",
            "type",
            "production_type",
            "unit",
            "status",
        }

        # see which of those actually exist
        present = set(mongo_df.columns) & _optional

        if _optional.issubset(mongo_df.columns):
            # 0. first, pick the columns you know exist, plus the always‑present ones
            cols = [
                "identifier", "version", "message_id",
                "values", "publication_date", "unavailability_type", "fuel_type",
                "affected_asset_or_unit_name", "affected_asset_or_unit_installed_capacity",
                "event_status"
            ] + list(_optional)
            mongo_df = mongo_df[cols]

            # 1. normalize “unit”
            unit_expanded = pd.json_normalize(mongo_df["unit"])
            mongo_df_2 = pd.concat([mongo_df.drop(columns=["unit"]), unit_expanded], axis=1)

            # 2. normalize the first element of “values”
            mongo_df_2["values_first"] = (
                mongo_df_2["values"]
                .apply(lambda lst: lst[0] if isinstance(lst, list) and lst else {})
            )
            values_expanded = pd.json_normalize(mongo_df_2["values_first"])
            mongo_df_2 = pd.concat(
                [mongo_df_2.drop(columns=["values", "values_first"]), values_expanded],
                axis=1
            )

            # 3. coalesce and drop old cols
            mongo_df_2["fuel_type"] = mongo_df_2["fuel_type"].combine_first(mongo_df_2["production_type"])
            mongo_df_2["publication_date"] = mongo_df_2["publication_date"].combine_first(mongo_df_2["updated_date"])
            mongo_df_2["event_status"] = mongo_df_2["event_status"].combine_first(mongo_df_2["status"])
            mongo_df_2["affected_asset_or_unit_installed_capacity"] = (
                mongo_df_2["affected_asset_or_unit_installed_capacity"]
                .combine_first(mongo_df_2["installed_capacity"])
            )
            mongo_df_2["affected_asset_or_unit_name"] = (
                mongo_df_2["affected_asset_or_unit_name"]
                .combine_first(mongo_df_2["name"])
            )
            mongo_df_2["unavailability_type"] = (
                mongo_df_2["unavailability_type"]
                .combine_first(mongo_df_2["type"].iloc[:, 0])
            )

            drop_cols = [
                "production_type",
                "updated_date",
                "status",
                "installed_capacity",
                "name",
                "type",
                "eic_code",
            ]
            # only drop those that are actually there
            drop_cols = [c for c in drop_cols if c in mongo_df_2.columns]
            mongo_df_2 = mongo_df_2.drop(columns=drop_cols)

            # now mongo_df_2 is your final
        else:
            # at least one of the required columns is missing:
            # present contains the ones you did find
            missing = _optional - present
            print(f"Skipping normalize process because these columns are missing: {missing}")
            mongo_df_2 = mongo_df.copy()  # or however you want to proceed

            mongo_df_2["values_first"] = mongo_df_2["values"].apply(
                lambda lst: lst[0] if isinstance(lst, list) and len(lst) > 0 else {}
            )

            # 2. Normalize that dict into separate columns
            values_expanded = pd.json_normalize(mongo_df_2["values_first"])
            #    e.g. this produces columns like “start_date”, “end_date”, etc.

            # 3. Concatenate back and drop the originals
            mongo_df_2 = pd.concat(
                [
                    mongo_df_2.drop(columns=["values", "values_first", "start_date", "end_date"]),
                    values_expanded
                ],
                axis=1
            )

        # Convert the date columns to datetime objects
        for col in ["publication_date", "start_date", "end_date"]:
            mongo_df_2[col] = pd.to_datetime(mongo_df_2[col], utc=True)

        # # Now convert everything to French time (CET/CEST):
        # for col in ["publication_date", "start_date", "end_date"]:
        #     mongo_df_2[col] = mongo_df_2[col].dt.tz_convert("Europe/Paris")

        # mongo_df_2 = mongo_df_2.drop_duplicates(subset='identifier', keep='first')
        mongo_df_2['version'] = mongo_df_2['version'].astype(float)
        # Sort by identifier and version to ensure the latest version is at the top
        # Method 1: Use groupby + idxmax to pick the row with the largest version per identifier
        idx = mongo_df_2.groupby("identifier")["version"].idxmax()
        mongo_df_2 = mongo_df_2.loc[idx].reset_index(drop=True)

        mongo_df_2 = mongo_df_2[mongo_df_2['event_status'] != 'DISMISSED']


        # Create the final dataframe
        final_df = pd.DataFrame()

        # Create the date column, with date range from start_date to end_date in daily granularity
        final_df['Date'] = pd.date_range(start=usr_start_date, end=usr_end_date, freq='D')
        final_df['Date'] = [ts.strftime("%Y-%m-%d") for ts in final_df['Date']]

        # For each plant create a new column with the plant name
        for plant, capacity in plants_metadata.items():
            # Create a new column for each plant
            final_df[plant] = np.nan  # Initialize with zeros

        mongo_df_3 = mongo_df_2.copy()

        dates_of_interest = list(pd.date_range(start=usr_start_date, end=usr_end_date, freq="D"))

        # Now convert each Timestamp → “YYYY-MM-DD” string:
        dates_of_interest = [ts.strftime("%Y-%m-%d") for ts in dates_of_interest]

        mongo_df_3['start_day'] = mongo_df_3['start_date'].dt.day
        mongo_df_3['start_hour'] = mongo_df_3['start_date'].dt.hour
        mongo_df_3['start_minute'] = mongo_df_3['start_date'].dt.minute
        mongo_df_3['end_day'] = mongo_df_3['end_date'].dt.day
        mongo_df_3['end_hour'] = mongo_df_3['end_date'].dt.hour
        mongo_df_3['end_minute'] = mongo_df_3['end_date'].dt.minute

        # mongo_df_3 = mongo_df_3.sort_values(by=['publication_date'], ascending=False)
        mongo_df_3 = mongo_df_3.sort_values(by=['publication_date'])

        # results_plants = {plant_name: {date: {"available_capacity": power, "publication_date": pd.to_datetime("1970-01-01", utc=True)} for date in dates_of_interest}
        #                 for plant_name, power in plants_metadata.items()}

        results_plants = {plant_name: {date: {"available_capacity": power, "publication_date": pd.to_datetime("1970-01-01", utc=True)} 
                                    for date in dates_of_interest}
                                        for plant_name, power in plants_metadata.items()}

        for row in mongo_df_3.itertuples():
            # Get the start and end dates for the unavailability
            row_start_date = str(row.start_date.date())
            row_end_date = str(row.end_date.date())
            
            # Get the plant name and capacity
            plant_name = row.affected_asset_or_unit_name
            plant_capacity = plants_metadata.get(plant_name, 0)  # Default to 0 if not found

            results_current_plant = results_plants[plant_name]

            power_unavailability = row.available_capacity
            publication_date_unav = row.publication_date
            
            for day in dates_of_interest:
                # percentage_of_day = results_current_plant[day]["percentage_of_day"]


                if row_start_date <= day <= row_end_date:
                    
                    # Check if the day is already updated with a later (more recent) update_date; by sorting the DataFrame by publication_date, 
                    # we ensure that the latest unavailability is applied
                    # Get the percentage of day that the plant is unavailable

                    # if day in final_df['Date'] and pd.notna(final_df.loc[final_df['Date'] == day, plant_name]).any():
                    if (day in results_current_plant) and (publication_date_unav <= results_current_plant[day]["publication_date"]):
                        # If the plant's capacity for that day is already set, skip to the next day
                        continue

                    # The unavailability starts and ends on the same day
                    if row_start_date == day and day == row_end_date:
                        percentage_of_day = (row.end_hour * 60 + row.end_minute - row.start_hour * 60 - row.start_minute) / (24 * 60)
                        # results_current_plant[day]["percentage_of_day"] += percentage_of_day
                        # power_of_day = percentage_of_day * row.available_capacity + (1 - percentage_of_day) * plant_capacity
                        # final_df.loc[final_df['Date'] == day, plant_name] = power_of_day

                    # The unavailability starts on the current day but ends on a later day
                    elif row_start_date == day and day < row_end_date:
                        percentage_of_day = (24 * 60 - (row.start_hour * 60 + row.start_minute)) / (24 * 60)
                        # results_current_plant[day]["percentage_of_day"] += percentage_of_day

                        # power_of_day = percentage_of_day * row.available_capacity + (1 - percentage_of_day) * plant_capacity
                        # final_df.loc[final_df['Date'] == day, plant_name] = power_of_day

                    # # The unavailability starts on a previous day and ends on the current day
                    elif row_end_date == day and row_start_date < day:
                        percentage_of_day = (row.end_hour * 60 + row.end_minute) / (24 * 60)
                        # results_current_plant[day]["percentage_of_day"] += percentage_of_day

                        # power_of_day = percentage_of_day * row.available_capacity + (1 - percentage_of_day) * plant_capacity
                        # final_df.loc[final_df['Date'] == day, plant_name] = power_of_day
                    
                    else:
                        # The unavailability starts on a previous day and ends on a later day
                        percentage_of_day = 1
                        # power_of_day = percentage_of_day * row.available_capacity + (1 - percentage_of_day) * plant_capacity
                        # final_df.loc[final_df['Date'] == day, plant_name] = power_of_day

                    power_of_day = percentage_of_day * power_unavailability + (1 - percentage_of_day) * plant_capacity

                    # Update the available_capacity for the day only if it's not already updated with a later update_date
                    if (day not in results_current_plant):
                        results_current_plant[day] = {"available_capacity": power_of_day, "publication_date": publication_date_unav}
                    
                    elif (day in results_current_plant) and (publication_date_unav > results_current_plant[day]["publication_date"]) \
                            and (power_of_day < results_current_plant[day]['available_capacity']):
                        # results_current_plant[day]["available_capacity"] *= power_of_day
                        # results_current_plant[day]["publication_date"] = publication_date_unav

                        results_current_plant[day] = {"available_capacity": power_of_day, "publication_date": publication_date_unav}

                    else:
                        continue

        output_results = {}
        for plant, plant_data in results_plants.items():
            available_capacity_per_day = {str(date): data["available_capacity"] for date, data in plant_data.items()}
            output_results[plant] = available_capacity_per_day

        self.add_total(output_results)

        output_results = {plant: {str(date): power for date, power in plant_data.items()} for plant, plant_data in output_results.items()}
        output_results = pd.DataFrame(output_results)

        # -------------------------------------------------
        # Calculate the average of each column excluding the last row
        averages = output_results.iloc[:-1, :].mean()

        # Replace the last row with the calculated averages
        output_results.iloc[-1, :] = averages

        output_results = output_results.to_dict()

        def turn_total_row_to_avg(data):
            # Replace the last key of each dictionary with 'Averages'
            for key, value in data.items():
                last_key = list(value.keys())[-1]
                value['Averages'] = value.pop(last_key)

        turn_total_row_to_avg(output_results)

        json_data = json.dumps(output_results)
        # print(json_data)
        return json_data
        # -------------------------------------------------


    def mongo_minmax_call(self):
        print("Starting mongo_minmax_call")

        # Use the same runtime connection configured by the main pipeline.
        client = self._mongo_client()
        results = []
        db = client["data"]
        collection = db["min_max_2015_2022"]

        # Retrieve all documents, excluding _id
        documents = collection.find({}, {'_id': 0})

        # Print each document
        for document in documents:
            results.append(document)

        # Close the database connection
        client.close()
        # print(results)
        return results

    def winter_forecast_graph(self, today, w_1, m_1, minmax):

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY WINTER PIPELINE --------------------------------- #
        current_date_str = str(datetime.datetime.now().strftime('%Y-%m-%d'))

        print("Data received from Flask:")
        df_nucmonitor = pd.DataFrame(today)
        df_one_m_before = pd.DataFrame(m_1)
        df_one_week_before = pd.DataFrame(w_1)
        df_minmax = pd.DataFrame(minmax)

        # Remove the final row 'Total'
        df_nucmonitor_2 = df_nucmonitor.iloc[:-1, :]
        # Get the last column
        df_nucmonitor_2 = df_nucmonitor_2.iloc[:, -1]

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY PIPELINE --------------------------------- #

        df_one_week_before = df_one_week_before.iloc[:-1, :]
        # Get the last column
        df_one_week_before = df_one_week_before.iloc[:, -1]

        df_one_m_before = df_one_m_before.iloc[:-1, :]
        # Get the last column
        df_one_m_before = df_one_m_before.iloc[:, -1]


        df_one_week_before.index = pd.to_datetime(df_one_week_before.index)
        df_one_m_before.index = pd.to_datetime(df_one_m_before.index)
        df_nucmonitor_2.index = pd.to_datetime(df_nucmonitor_2.index)


        # avg_w_1 = df_one_week_before.resample('ME').mean()
        # avg_w_1.index = avg_w_1.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_w_1 = df_one_week_before.reset_index()
        avg_w_1 = avg_w_1.rename(columns={'index': 'Dates'})

        # avg_m_1 = df_one_m_before.resample('ME').mean()
        # avg_m_1.index = avg_m_1.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_m_1 = df_one_m_before.reset_index()
        avg_m_1 = avg_m_1.rename(columns={'index': 'Dates'})

        # avg_nucm = df_nucmonitor_2.resample('ME').mean()
        # avg_nucm.index = avg_nucm.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_nucm = df_nucmonitor_2.reset_index()
        avg_nucm = avg_nucm.rename(columns={'index': 'Dates'})

        merged_df = pd.merge(avg_nucm[['Dates', 'Total']], avg_w_1[['Dates', 'Total']], on='Dates', how='outer')

        # Rename the columns with the specified suffixes
        merged_df = merged_df.rename(columns={'Total_x': f'Forecast as of {current_date_str}', 'Total_y': 'Forecast as of Week -1'})

        merged_df = pd.merge(merged_df, avg_m_1[['Dates', 'Total']], on='Dates', how='outer')
        merged_df.rename(columns={'Total': 'Forecast as of Month -1'}, inplace=True)


        merged_df_2 = merged_df.copy()
        merged_df_2['Dates'] = merged_df_2['Dates'].astype("string")

        # Get Month
        merged_df_2["mm-dd"] = merged_df_2['Dates'].str[5:]
        
        df_minmax_2 = df_minmax.copy()

        # Assuming df is your DataFrame
        if '02-29' not in merged_df_2['mm-dd'].values:
            df_minmax_2 = df_minmax_2[df_minmax_2['mm-dd'] != '02-29']

        merged_df_3 = pd.merge(merged_df_2, df_minmax_2, on='mm-dd', how='left')

        merged_df_3 = merged_df_3.copy().drop(columns=["mm-dd"])

        merged_df_3.rename(columns={'min': 'Historical Min 2015-2022', 'max': 'Historical Max 2015-2022'}, inplace=True)
        # merged_df_3 = merged_df_3.to_dict()
        # json_data = json.dumps(merged_df_3)
        # return json_data
        # Divide all values in selected columns by 1000
        merged_df_3.iloc[:, 1:] = merged_df_3.iloc[:, 1:].div(1000)

        # Round decimals to the tenth
        merged_df_3.iloc[:, 1:] = merged_df_3.iloc[:, 1:].round(1)
        return merged_df_3

    def nuctracker_forecast_graph(self, today_unavs, m_1_unavs, today_prod, predicted_prod, minmax):

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY WINTER PIPELINE --------------------------------- #
        current_date_str = str(datetime.datetime.now().strftime('%Y-%m-%d'))

        print("Data received from Flask:")
        df_nucmonitor = pd.DataFrame(today_unavs)
        df_one_m_before = pd.DataFrame(m_1_unavs)
        # df_one_week_before = pd.DataFrame(w_1_unavs)
        df_minmax = pd.DataFrame(minmax)
        df_production = pd.DataFrame(today_prod)
        df_pred_production = pd.DataFrame(predicted_prod)

        # Remove the final row 'Total'
        df_nucmonitor_2 = df_nucmonitor.iloc[:-1, :]
        # Get the last column
        df_nucmonitor_2 = df_nucmonitor_2.iloc[:, -1]

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY PIPELINE --------------------------------- #

        df_one_m_before = df_one_m_before.iloc[:-1, :]
        # Get the last column
        df_one_m_before = df_one_m_before.iloc[:, -1]

        # df_one_m_before = df_one_m_before.iloc[:-1, :]
        # # Get the last column
        # df_one_m_before = df_one_m_before.iloc[:, -1]

        # Production Totals

        df_production_totals = df_production[['Date', 'Total']]
        df_production_totals = df_production_totals.rename(columns={'Date': 'Dates'})

        df_production_totals.set_index('Dates', inplace=True)

        df_production_totals.index = pd.to_datetime(df_production_totals.index)

        df_pred_production.set_index('Dates', inplace=True)

        df_pred_production.index = pd.to_datetime(df_pred_production.index)


        df_one_m_before.index = pd.to_datetime(df_one_m_before.index)
        # df_one_m_before.index = pd.to_datetime(df_one_m_before.index)
        df_nucmonitor_2.index = pd.to_datetime(df_nucmonitor_2.index)


        # avg_w_1 = df_one_week_before.resample('ME').mean()
        # avg_w_1.index = avg_w_1.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_m_1 = df_one_m_before.reset_index()
        avg_m_1 = avg_m_1.rename(columns={'index': 'Dates'})

        # avg_m_1 = df_one_m_before.resample('ME').mean()
        # avg_m_1.index = avg_m_1.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        # avg_m_1 = df_one_m_before.reset_index()
        # avg_m_1 = avg_m_1.rename(columns={'index': 'Dates'})

        # avg_nucm = df_nucmonitor_2.resample('ME').mean()
        # avg_nucm.index = avg_nucm.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_nucm = df_nucmonitor_2.reset_index()
        avg_nucm = avg_nucm.rename(columns={'index': 'Dates'})

        merged_df = pd.merge(avg_nucm[['Dates', 'Total']], avg_m_1[['Dates', 'Total']], on='Dates', how='outer')

        # Rename the columns with the specified suffixes
        merged_df = merged_df.rename(columns={'Total_x': f'Forecast as of {current_date_str}', 'Total_y': 'Forecast as of Month -1'})

        # merged_df = pd.merge(merged_df, avg_m_1[['Dates', 'Total']], on='Dates', how='outer')
        # merged_df.rename(columns={'Total': 'Forecast as of Month -1'}, inplace=True)

        # Get production in merge
        merged_df = pd.merge(merged_df, df_production_totals, on='Dates', how='outer')
        merged_df.rename(columns={'Total': 'Actual Production'}, inplace=True)

        # Get production in merge
        merged_df = pd.merge(merged_df, df_pred_production, on='Dates', how='outer')
        merged_df.rename(columns={'Total': 'Predicted Production'}, inplace=True)


        merged_df_2 = merged_df.copy()
        merged_df_2['Dates'] = merged_df_2['Dates'].astype("string")

        # Get Month
        merged_df_2["mm-dd"] = merged_df_2['Dates'].str[5:]
        
        df_minmax_2 = df_minmax.copy()

        # Making sure leap day doesn't fuck things up
        if '02-29' not in merged_df_2['mm-dd'].values:
            df_minmax_2 = df_minmax_2[df_minmax_2['mm-dd'] != '02-29']

        merged_df_3 = pd.merge(merged_df_2, df_minmax_2, on='mm-dd', how='left')

        merged_df_3 = merged_df_3.copy().drop(columns=["mm-dd"])

        merged_df_3.rename(columns={'min': 'Historical Min 2015-2022', 'max': 'Historical Max 2015-2022'}, inplace=True)
        # merged_df_3 = merged_df_3.to_dict()
        # json_data = json.dumps(merged_df_3)
        # return json_data
        # Divide all values in selected columns by 1000
        merged_df_3.iloc[:, 1:] = merged_df_3.iloc[:, 1:].div(1000)

        # Round decimals to the tenth
        merged_df_3.iloc[:, 1:] = merged_df_3.iloc[:, 1:].round(1)
        return merged_df_3

    def winter_forecast_table(self, today, w_1, m_1):

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY WINTER PIPELINE --------------------------------- #
        current_date_str = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        print("Data received from Flask:")
        df_nucmonitor = pd.DataFrame(today)
        df_one_m_before = pd.DataFrame(m_1)
        df_one_week_before = pd.DataFrame(w_1)

        df_nucmonitor = df_nucmonitor.iloc[:-1, :]
        # Get the last column
        df_nucmonitor = df_nucmonitor.iloc[:, -1]

        df_one_week_before = df_one_week_before.iloc[:-1, :]
        # Get the last column
        df_one_week_before = df_one_week_before.iloc[:, -1]

        df_one_m_before = df_one_m_before.iloc[:-1, :]
        # Get the last column
        df_one_m_before = df_one_m_before.iloc[:, -1]

        df_one_week_before.index = pd.to_datetime(df_one_week_before.index)
        df_one_m_before.index = pd.to_datetime(df_one_m_before.index)

        df_nucmonitor.index = pd.to_datetime(df_nucmonitor.index)

        avg_w_1_table = df_one_week_before.resample('ME').mean()
        avg_w_1_table.index = avg_w_1_table.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_w_1_table = avg_w_1_table.reset_index()
        avg_w_1_table = avg_w_1_table.rename(columns={'index': 'Dates'})

        avg_m_1_table = df_one_m_before.resample('ME').mean()
        avg_m_1_table.index = avg_m_1_table.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_m_1_table = avg_m_1_table.reset_index()
        avg_m_1_table = avg_m_1_table.rename(columns={'index': 'Dates'})

        avg_nucm_table = df_nucmonitor.resample('ME').mean()
        avg_nucm_table.index = avg_nucm_table.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_nucm_table = avg_nucm_table.reset_index()
        avg_nucm_table = avg_nucm_table.rename(columns={'index': 'Dates'})

        # Merge avg_nucm with avg_w_1
        merged_df_table = pd.merge(avg_m_1_table[['Dates', 'Total']], avg_w_1_table[['Dates', 'Total']], on='Dates', how='outer')

        # Rename the columns with the specified suffixes
        merged_df_table = merged_df_table.rename(columns={'Total_x': f'Average Forecast as of Month -1', 'Total_y': 'Average Forecast as of Week -1'})

        # Merge with avg_m_1
        merged_df_table = pd.merge(merged_df_table, avg_nucm_table[['Dates', 'Total']], on='Dates', how='outer')
        merged_df_table.rename(columns={'Total': f"Average Forecast as of {current_date_str}"}, inplace=True)

        merged_df_table_slope_2 = merged_df_table.copy()

        # Calculate the slopes and add them to a new column 'Trends'
        def calculate_slope(row):
            x = range(1, len(row))
            y = row[1:]
            slope = (y[-1] - y[0]) / (x[-1] - x[0])
            return slope

        # Transform 'Trends' column based on conditions
        def categorize_trends(value):
            if value > 0:
                return 'Positive'
            elif value == 0:
                return 'Neutral'
            else:
                return 'Negative'
        # Divide all values in selected columns by 1000
        merged_df_table_slope_2.iloc[:, 1:] = merged_df_table_slope_2.iloc[:, 1:].div(1000)

        # Round decimals to the tenth
        merged_df_table_slope_2.iloc[:, 1:] = merged_df_table_slope_2.iloc[:, 1:].round(1)

        merged_df_table_slope_2['Trends'] = merged_df_table_slope_2.apply(calculate_slope, axis=1)

        merged_df_table_slope_2['Trends'] = merged_df_table_slope_2['Trends'].apply(categorize_trends)
        # merged_df_table_slope_2 = merged_df_table_slope_2.to_dict()
        # json_data = json.dumps(merged_df_table_slope_2)
        # return json_data
        return merged_df_table_slope_2
    

    def nuctracker_forecast_table(self, today, m_1):

        # --------------------------------- AVERAGE EXPECTED AVAILABILITY WINTER PIPELINE --------------------------------- #
        current_date_str = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        print("Data received from Flask:")
        df_nucmonitor = pd.DataFrame(today)
        df_one_m_before = pd.DataFrame(m_1)
        # df_one_week_before = pd.DataFrame(w_1)
        # df_production = pd.DataFrame(today_prod)

        df_nucmonitor = df_nucmonitor.iloc[:-1, :]
        # Get the last column
        df_nucmonitor = df_nucmonitor.iloc[:, -1]

        df_one_m_before = df_one_m_before.iloc[:-1, :]
        # Get the last column
        df_one_m_before = df_one_m_before.iloc[:, -1]

        # df_one_m_before = df_one_m_before.iloc[:-1, :]
        # # Get the last column
        # df_one_m_before = df_one_m_before.iloc[:, -1]

        df_one_m_before.index = pd.to_datetime(df_one_m_before.index)
        # df_one_m_before.index = pd.to_datetime(df_one_m_before.index)

        df_nucmonitor.index = pd.to_datetime(df_nucmonitor.index)


        # Production Totals

        # df_production_totals = df_production[['Date', 'Total']]
        # df_production_totals = df_production_totals.rename(columns={'Date': 'Dates'})

        # df_production_totals.set_index('Dates', inplace=True)

        # df_production_totals.index = pd.to_datetime(df_production_totals.index)
        # df_production_totals.index = df_production_totals.index.strftime('%Y-%m')


        avg_m_1_table = df_one_m_before.resample('ME').mean()
        avg_m_1_table.index = avg_m_1_table.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_m_1_table = avg_m_1_table.reset_index()
        avg_m_1_table = avg_m_1_table.rename(columns={'index': 'Dates'})

        # avg_m_1_table = df_one_m_before.resample('ME').mean()
        # avg_m_1_table.index = avg_m_1_table.index.strftime('%Y-%m')
        # # # Reset the index and create a new DataFrame with the old index as a column
        # avg_m_1_table = avg_m_1_table.reset_index()
        # avg_m_1_table = avg_m_1_table.rename(columns={'index': 'Dates'})

        avg_nucm_table = df_nucmonitor.resample('ME').mean()
        avg_nucm_table.index = avg_nucm_table.index.strftime('%Y-%m')
        # # Reset the index and create a new DataFrame with the old index as a column
        avg_nucm_table = avg_nucm_table.reset_index()
        avg_nucm_table = avg_nucm_table.rename(columns={'index': 'Dates'})

        # # Merge avg_nucm with avg_w_1
        # merged_df_table = pd.merge(avg_m_1_table[['Dates', 'Total']], avg_w_1_table[['Dates', 'Total']], on='Dates', how='outer')

        # # Rename the columns with the specified suffixes
        # merged_df_table = merged_df_table.rename(columns={'Total_x': f'Average Forecast as of Month -1', 'Total_y': 'Average Forecast as of Week -1'})

        # Merge with avg_m_1
        # merged_df_table = pd.merge(merged_df_table, avg_nucm_table[['Dates', 'Total']], on='Dates', how='outer')
        # merged_df_table.rename(columns={'Total': f"Average Forecast as of {current_date_str}"}, inplace=True)
        merged_df_table = pd.merge(avg_m_1_table[['Dates', 'Total']], avg_nucm_table[['Dates', 'Total']], on='Dates', how='outer')
        merged_df_table = merged_df_table.rename(columns={'Total_x': f'Average Forecast as of Month -1', 'Total_y': f"Average Forecast as of {current_date_str}"})

        # Merge with df_production_totals
        # merged_df_table = pd.merge(merged_df_table, df_production_totals, on='Dates', how='outer')
        # merged_df_table.rename(columns={'Total': "Actual Production"}, inplace=True)

        merged_df_table = merged_df_table.groupby('Dates').mean().reset_index()

        merged_df_table_slope_2 = merged_df_table.copy()

        # Calculate the slopes and add them to a new column 'Trends'
        def calculate_slope(row):
            x = range(1, len(row))
            y = row[1:]
            slope = (y.iloc[-1] - y.iloc[0]) / (x[-1] - x[0])
            return slope

        # Transform 'Trends' column based on conditions
        def categorize_trends(value):
            if value > 0:
                return 'Positive'
            elif value == 0:
                return 'Neutral'
            else:
                return 'Negative'
            
        # Divide all values in selected columns by 1000
        merged_df_table_slope_2.iloc[:, 1:] = merged_df_table_slope_2.iloc[:, 1:].div(1000)

        # Round decimals to the tenth
        merged_df_table_slope_2.iloc[:, 1:] = merged_df_table_slope_2.iloc[:, 1:].round(1)

        merged_df_table_slope_2['Trends'] = merged_df_table_slope_2.apply(calculate_slope, axis=1)

        merged_df_table_slope_2['Trends'] = merged_df_table_slope_2['Trends'].apply(categorize_trends)
        # merged_df_table_slope_2 = merged_df_table_slope_2.to_dict()
        # json_data = json.dumps(merged_df_table_slope_2)
        # return json_data
        return merged_df_table_slope_2
