from geopy.extra.rate_limiter import RateLimiter
import concurrent.futures
import math
import time
from tqdm import tqdm
import psycopg2
import pandas as pd
from geopy import Point
import os
import csv
import plotly.graph_objects as go
import plotly.express as px
from geopy.distance import geodesic
import requests
import json
import os

def raw_data_to_csv():
    # input addresses into database
    property_data = []

    ogdf = pd.read_csv('mod_Attom.csv')
    ogdf = ogdf.iloc[:, [0] + list(range(36, 48))]

    # build list of addresses by processing individual address units
    # this is where data cleaning for the address data should happen
    ids = []
    bad_zips = []
    bad_nums = []
    for index, row in ogdf.iterrows():
        address_string = ""
        for i, col in enumerate(row[1:]):
            if type(col) != float:
                address_string += str(col) + " "
            if i == 5 or (i == 7 and type(col) != float) or i == 8:
                address_string = address_string[:-1]
                address_string += ", "

        address_string = address_string[:-1]
        #print(address_string)
        address_tuple = (address_string, address_string[-5:], 0, 0, row[0])
        #print(type(row["SA_SITE_HOUSE_NBR"]))
        if row[0] not in ids:
            if row['SA_SITE_ZIP'] < 90000:
                bad_zips.append(address_tuple)
                continue
            if row['SA_SITE_HOUSE_NBR'] == "" or type(row["SA_SITE_HOUSE_NBR"]) == float or row["SA_SITE_HOUSE_NBR"][0].isalpha():
                bad_nums.append(address_tuple)
                continue
            #only valid and clean addresses are added to list
            property_data.append(address_tuple)
            ids.append(row[0])

    print(f'{len(ids)} unique addresses')
    print(f'{len(bad_zips)} rows were removed due to unclean zip codes')
    print(f'{len(bad_nums)} rows were removed due to missing or invalid address numbers')
    with open('address_list.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerows(property_data)



def csv_to_db(cursor):
    if not os.path.isfile('./address_list.csv'):
        print("Address List has not yet been built")
        return
    
    with open('address_list.csv', 'r') as f:
        reader = csv.reader(f)
        property_data = [tuple(row) for row in reader]

    print(property_data)

    insert_data_query = """
    INSERT INTO addresses (address, zip, nearest_street_pri, nearest_street_sec, address_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_data_query, property_data)




def input_sample_geocodes(cursor):

    df = pd.read_csv('geocoded_sample_tenderloin.csv')
    no_match = []

    for address in df["original_address"]:
        select_query = f"""
        SELECT * FROM addresses WHERE address = '{address}'
        """

        cursor.execute(select_query)
        row = cursor.fetchone()

        if row:
            columns = df[df["original_address"] == address].iloc[:, 1:3]
            print(columns)
            long = columns.iloc[0, 1]
            lat = columns.iloc[0, 0]
            # print(lat, long)

            update_query = "UPDATE addresses SET longitude = %s , latitude = %s WHERE address = %s;"
            cursor.execute(update_query, (long, lat, address))
        else:
            no_match.append(address)

    print(f'{len(no_match)} addresses were not found in the database')
    print(no_match)



#must input indices 
def batch_geocode(cursor, num_to_geocode, api_key):
    #get db entries that have no lat/lon

    cursor.execute(f'SELECT address, address_id FROM addresses WHERE latitude is NULL LIMIT {num_to_geocode}')
    to_api = cursor.fetchall()
    addresses, ids = zip(*to_api)

    post_url = f"https://api.geoapify.com/v1/batch/geocode/search?filter=rect:-122.522345,37.696018,-122.350684,37.816266&apiKey={api_key}"
    headers = {"Content-Type": "application/json"}

    try:
        resp = requests.post(post_url, headers=headers, data=json.dumps(addresses))
        print(resp)
        job_id = resp.json()['id']

    except requests.exceptions.HTTPError as e:
        print(e.response.text)
        return


    get_url = f"https://api.geoapify.com/v1/batch?id={job_id}&apiKey={api_key}"
    valid = False
    while not valid:
        try:
            resp = requests.get(get_url, headers=headers)
            result = resp.json().get("results", [])
            if len(result) > 0:
                valid = True

        except requests.exceptions.HTTPError as e:
            print(e.response.text)
        print("waiting another 20 seconds...")
        time.sleep(20)
            
        
    #build tuples
    geocoded_data = []
    print(resp)
    for x in zip(result, ids):
        if len(x[0]["result"]["features"]) >= 1:
            properties = x[0]["result"]["features"][0]["properties"]
            geocoded_tuple = (properties["lon"],
                            properties["lat"], 
                            properties["rank"]["confidence"], 
                            x[1])
            geocoded_data.append(geocoded_tuple)

    print(geocoded_data)

    update_query = "UPDATE addresses SET longitude = %s , latitude = %s , confidence = %s WHERE address_id = %s;"

    cursor.executemany(update_query, geocoded_data)



if __name__ == "__main__":

    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password=os.getenv("DBPASS", "password"),
        database="sf_property_mapping"
    )
    cursor = connection.cursor()

    #raw_data_to_csv()

    #csv_to_db(cursor)

    api_key = os.getenv("GEOAPIKEY", 'APIKEY')
    batch_geocode(cursor, 900, api_key)

    #input_sample_geocodes(cursor)

    connection.commit()
    cursor.close()
    connection.close()
