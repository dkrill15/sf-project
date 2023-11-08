#!/usr/bin/env python3

from rtree import index
import geopy.distance
from geopy.distance import geodesic
import plotly.express as px
import plotly.graph_objects as go
import csv
import json
from geopy.geocoders import Nominatim
import pandas as pd


# locator = Nominatim(user_agent="my_app")

# #read CSV from modified txt file download
# ogdf = pd.read_csv('./mod_Attom.csv')
# ogdf = ogdf.iloc[:, [0] + list(range(36, 48))]
# df = ogdf[ogdf['SA_SITE_ZIP'] == 94102]
# df = pd.concat([df, ogdf[ogdf['SA_SITE_ZIP'] == 94109]], ignore_index=True)

# #build list of addresses by processing individual address units
# addresses = []
# for index, row in df.iterrows():
#     address_string = ""
#     for i, col in enumerate(row[1:]):
#         if type(col) != float:
#             address_string += str(col) + " "
#         if i == 5 or (i == 7 and type(col) != float) or i == 8:
#             address_string = address_string[:-1]
#             address_string += ", "

#     address_string = address_string[:-1]
#     addresses.append([address_string, row[0]])

# print(addresses)

# #write addresses + lat/lon to csv
# f = open('./address_to_geocode.csv', 'w')
# fwriter = csv.writer(f)
# fwriter.writerow(["Long", "Lat", "Address", "ID"])
# no_maps = 0
# count = 0
# for address in addresses:
#     print(address)
#     try:
#         y = locator.geocode(address[0])
#         if y:
#             fwriter.writerow([y.longitude, y.latitude, address[0], address[1]])
#         count += 1
#     except:
#         print("Address could not be mapped to a geolocation")
#         no_maps += 1
#     if count > 45:
#         break

# f.close()

# print(f'found {len(addresses)} addresses in the bounding box, of which {no_maps} could not be mapped to a geolocation ({100 - no_maps*100/len(addresses)}% success)')


# re-read addresses as dataframe and plot
address_df = pd.read_csv("./address_to_geocode.csv")
# print(address_df)
# fig = px.scatter_mapbox(address_df,
#                         lat="Lat",
#                         lon="Long",
#                         hover_name="Address",
#                         zoom=12,
#                         height=800,
#                         width=800)


WEST_END = -122.4276
EAST_END = -122.401686
NORTH_END = 37.791812
SOUTH_END = 37.77075
# add bounding box
# fig.add_trace((go.Scattermapbox(
#     mode="lines", fill="toself",
#     lon=[WEST_END, WEST_END, EAST_END, EAST_END, WEST_END],
#     lat=[SOUTH_END, NORTH_END, NORTH_END, SOUTH_END, SOUTH_END])))


with open('segments.json', 'r') as file:
    streets = json.load(file)

polygons = []
for item in streets:
    polygon = {"lat": [], "lon": []}
    in_bounds = False
    for coor in streets[item]['coordinates']:
        lat = coor[0][0]
        lon = coor[0][1]
        if lon >= WEST_END or lon <= EAST_END or lat >= SOUTH_END or lat <= NORTH_END:
            in_bounds = True
        polygon["lat"].append(coor[0][0])
        polygon["lon"].append(coor[0][1])
    if in_bounds:
        polygons.append(polygon)

# print(f'found {len(polygons)} streets in the bounding box')

# colors = [
#     'red',
#     'green',
#     'blue',
#     'yellow',
#     'cyan',
#     'magenta',
#     'gray',
#     'orange',
#     'darkgreen',
#     'purple'
# ]

# for i, polygon_data in enumerate(polygons):
#     fig.add_trace(go.Scattermapbox(
#         mode="markers+lines",
#         lon=polygon_data['lon'],
#         lat=polygon_data['lat'],
#         line={'color':colors[i%10]}
#     ))

# fig.update_layout(mapbox_style="open-street-map")
# fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

# fig.show()


# preprocessing of list data
street_list = []

for item in streets:
    temp_list = []
    for coor in streets[item]['coordinates']:
        lat = coor[0][0]
        lon = coor[0][1]
        temp_list.append((lat, lon))
    street_list.append(temp_list)

col1 = address_df['Long'].astype(float)
col2 = address_df['Lat'].astype(float)
property_locations = list(zip(col2, col1))

# create a spatial index for the segments
spatial_index = index.Index()
# Initialize variables to store the nearest segment and its distance
nearest_segments = []
min_distances = []

print("making r tree")

# Populate the spatial index with segments
for segment_id, segment in enumerate(street_list):
    for i in range(len(segment) - 1):
        p1 = segment[i]
        p2 = segment[i + 1]
        min_x = min(p1[1], p2[1])
        max_x = max(p1[1], p2[1])
        min_y = min(p1[0], p2[0])
        max_y = max(p1[0], p2[0])
        spatial_index.insert(segment_id, (min_x, min_y, max_x, max_y))

# Custom function to calculate the distance from a point to a line segment

print("finished making r tree")


def distance_to_segment(point, segment):
    print(segment)
    p1 =
    distance = geopy.distance.geodesic(point, p1).miles

    if p1 != p2:
        line_distance = geopy.distance.geodesic(p1, p2).miles
        u = ((point[1] - p1[1]) * (p2[1] - p1[1]) + (point[0] -
             p1[0]) * (p2[0] - p1[0])) / (line_distance ** 2)

        if 0 <= u <= 1:
            return abs(distance * (1 - u))
        else:
            return min(geopy.distance.geodesic(point, p1).miles, geopy.distance.geodesic(point, p2).miles)


for location in property_locations:
    min_distance = float('inf')
    nearest_segment = None

    # Calculate the candidate segments using the spatial index
    for segment_id in spatial_index.intersection((location[1], location[0], location[1], location[0])):
        segment = street_list[segment_id]

        for i in range(len(segment) - 1):
            p1 = segment[i]
            p2 = segment[i + 1]

            # Calculate the distance to the segment
            dist = distance_to_segment(location, segment)

            if dist < min_distance:
                min_distance = dist
                nearest_segment = segment

    nearest_segments.append(nearest_segment)
    min_distances.append(min_distance)

# Print the results
for i, location in enumerate(property_locations):
    print(
        f"Location {location} is nearest to segment {nearest_segments[i]} with a distance of {min_distances[i]} miles.")


# for loc in property_locations:
#     print(loc)
#     nearest_segment, min_distance = find_nearest_segment(loc, street_list)
#     print(
#         f"Location {loc} is nearest to segment {nearest_segment} with a distance of {min_distance} miles.")
