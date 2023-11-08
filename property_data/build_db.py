from geopy.extra.rate_limiter import RateLimiter
import concurrent.futures
import math
import time
from tqdm import tqdm
import psycopg2
import pandas as pd
from geopy import Point
from geopy.geocoders import Nominatim
import csv
import plotly.graph_objects as go
import plotly.express as px
from geopy.distance import geodesic

def build_database(csv_exists):
    # input addresses into database
    property_data = []

    if not csv_exists:
        ogdf = pd.read_csv('mod_Attom.csv')
        ogdf = ogdf.iloc[:, [0] + list(range(36, 48))]

        # build list of addresses by processing individual address units
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
            print(address_string)
            address_tuple = (address_string, address_string[-5:], 0, 0, row[0])
            if row['SA_SITE_ZIP'] < 90000:
                bad_zips.append(address_tuple)
                continue
            if row['SA_SITE_HOUSE_NBR'] == "":
                bad_nums.append(address_tuple)
                continue
            if row[0] not in ids:
                property_data.append(address_tuple)
                ids.append(row[0])

        print(f'{len(ids)} unique addresses')
        print(f'{len(bad_zips)} rows were removed due to unclean zip codes')
        print(
            f'{len(bad_nums)} rows were removed due to missing or invalid address numbers')
        with open('address_list.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerows(property_data)
    else:
        with open('address_list.csv', 'r') as f:
            reader = csv.reader(f)
            property_data = [tuple(row) for row in reader]

    print(property_data)

    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PASSWORD",
        database="sf_property_mapping"
    )

    cursor = connection.cursor()

    insert_data_query = """
    INSERT INTO addresses (address, zip, nearest_street_pri, nearest_street_sec, address_id)
    VALUES (%s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_data_query, property_data)
    connection.commit()
    connection.close()


locator = Nominatim(user_agent="my_app")


def input_sample_geocodes():
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="PASSWORD",
        database="sf_property_mapping"
    )
    cursor = connection.cursor()

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

    connection.commit()
    cursor.close()
    connection.close()


def geocode_custom(address):
    print(address[0])
    y = locator.geocode(address[0], country_codes='US', viewbox=[
        Point(37.69994, -122.517181), Point(37.812045, -122.354961)], bounded=True)
    print("found y")
    if y:
        return [y.longitude, y.latitude, address[0], address[1]]
    return []


def do_geocode():
    # TBD - look at options for batch geocoding

    # # Create a ThreadPoolExecutor
    # with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    #     # List to store results
    #     results = []

    #     # Wrap the DataFrame processing with tqdm for a progress bar
    #     for address in tqdm(addresses, desc="Geocoding"):
    #         future = executor.submit(geocode_custom, address[0])
    #         results.append(future)

    #     # Wait for all threads to complete
    #     concurrent.futures.wait(results)

    # # Collect the results
    # geocoded_data = [result.result() for result in results]

    # print(geocoded_data)

    geocode = RateLimiter(geocode_custom, min_delay_seconds=2)

    # with tqdm(total=len(addresses)) as pbar:
    #     with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
    #         futures = [ex.submit(geocode, address) for address in addresses[:50]]
    #         for future in concurrent.futures.as_completed(futures):
    #             result = future.result()
    #             pbar.update(1)

    # with concurrent.futures.ThreadPoolExecutor() as e:
    #     locations = list(
    #         e.map(geocode, [address[0] for address in addresses]))

    # for address in tqdm(addresses, desc="Geocoding Addresses..."):
    #     try:
    #         y = geocode(address)

    #         if y:
    #             fwriter.writerow([y.longitude, y.latitude, address[0], address[1]])
    #         count += 1
    #     except:
    #         print("Address could not be mapped to a geolocation")
    #         no_maps += 1

    # f.close()

    # print(f'read {len(addresses)} addresses, of which {no_maps} could not be mapped to a geolocation ({100* (1 - no_maps/len(addresses))}% success)')


if __name__ == "__main__":
    # build_database(True)

    input_sample_geocodes()
