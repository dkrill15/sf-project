import geopy.distance
import plotly.express as px
import math
import psycopg2
import os
from tqdm import tqdm

def haversine(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # Radius of the Earth in kilometers
    earth_radius = 6371.0

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = earth_radius * c

    return distance


def get_nearest_segment(target, locations):
    min_distance = float('inf')
    closest_p, closest_s = None, None

    for location in locations:
        lat, lon = location[0], location[1]
        distance = haversine(target[1], target[2], lat, lon)
        if distance < min_distance:
            min_distance = distance
            closest_p, closest_s = location[2], location[3]

    #print(min_distance)

    return closest_p, closest_s, min_distance


def find_nearest():
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password=os.getenv("DBPASS", "password"),
        database="sf_property_mapping"
    )
    cursor = connection.cursor()

    # query unique street names
    select_unique_streets = """
    SELECT DISTINCT street_name FROM street_points
    """

    cursor.execute(select_unique_streets)
    street_names = [x[0] for x in cursor.fetchall()]
    # with_intersections = len(street_names)
    # intersections = [x[0] for x in street_names if ',' in x[0]]
    # print(intersections)
    # street_names = [x[0] for x in street_names if ',' not in x[0]]
    # without_intersections = len(street_names)
    # print(f'{with_intersections-without_intersections} streets are identified as being of two streets')

    nearest_info = []

    for street in (pbar := tqdm(street_names)):

        og_street = street
        if " " in street:
            street = str.upper(" ".join(street.split()[:-1]))
        else:
            street = str.upper(street)
        print(f'Orignal: {og_street} \t Modified: {street}')
        
        pbar.set_postfix_str(street)

        #print(street)

        if "'" in street or "NaN" in og_street:
            continue

        # get all lat/lon coord from addresses w street in name
        select_address_coords = f"""
        SELECT address_id, latitude, longitude, address, distance FROM addresses WHERE address LIKE '%{street}%';
        """

        cursor.execute(select_address_coords)
        prop_coords = [list(x) for x in cursor.fetchall()]

        # get all lat/lon coord from street_points w street in name
        select_street_coords = f"""
        SELECT * FROM street_points WHERE street_name = '{og_street}';
        """

        cursor.execute(select_street_coords)
        street_coords = [list(x) for x in cursor.fetchall()]
        # print(street_coords)

        print(f'Found {len(prop_coords)} address and {len(street_coords)} potential coordinates')

        for p in prop_coords:
            if p[1] is not None:

                seg_prim, seg_sec, min_dist = get_nearest_segment(p, street_coords)

                if p[4] == 0 or min_dist < p[4]:
                    nearest_info.append((seg_prim, seg_sec, min_dist, p[0]))

    update_query = "UPDATE addresses SET nearest_street_pri = %s , nearest_street_sec = %s , distance = %s WHERE address_id = %s;"

    cursor.executemany(update_query, nearest_info)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    find_nearest()
