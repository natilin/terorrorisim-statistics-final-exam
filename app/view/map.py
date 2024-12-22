import math

import folium
from folium.plugins import MarkerCluster

from app.repository.mongo_repository import get_israel_terror

SF_COORDINATES = (37.76, -122.45)

israel_list = get_israel_terror()
# for speed purposes
MAX_RECORDS = 1000

_map = folium.Map(location=(31.506981, 34.718904), zoom_start=12)
# create empty map zoomed in on San Francisco
marker_cluster = MarkerCluster().add_to(_map)

# add a marker for every record in the filtered data, use a clustered view
for each in israel_list:
    if not math.isnan(each["location"]["latitude"]) and not math.isnan(each["location"]["longitude"]):
        folium.Marker(
            location = [each["location"]["latitude"],each["location"]["longitude"]],
            clustered_marker = True).add_to(marker_cluster)

_map.save("map.html")