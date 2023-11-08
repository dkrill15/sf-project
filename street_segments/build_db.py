import psycopg2
import json

connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="unebarque3",
    database="sf_property_mapping"
)

cursor = connection.cursor()

insert_data_query = """
INSERT INTO street_points (street_id_primary, street_id_secondary, latitude, longitude, street_name)
VALUES (%s, %s, %s, %s, %s)
"""


# process data from csv
with open('segments.json', 'r') as file:
    streets = json.load(file)


street_data = []
for item in streets:
    street_id = json.loads(streets[item]['segment_id'])
    street_name = streets[item]['name']
    for coor in streets[item]['coordinates']:
        lat = coor[0][0]
        lon = coor[0][1]
        street_data.append(
            (street_id[0], street_id[1], lat, lon, street_name)
        )

print(len(street_data))

# for data in street_data:
#     print(data)
#     cursor.execute(insert_data_query, data)

cursor.executemany(insert_data_query, street_data)

connection.commit()
connection.close()
