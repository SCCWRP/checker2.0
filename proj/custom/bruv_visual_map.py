import folium
import geopandas
import pandas as pd
import json
import os
from shapely.geometry import Point
from matplotlib import colors
import branca
from .bruv import bruv
from inspect import currentframe
from flask import current_app
from .functions import checkData, get_badrows


def bruv_visual_map(all_dfs, spatialtable):
    testdf = all_dfs.get(spatialtable)

    testdf1=testdf.dropna(subset=['latitude', 'longitude']) #dropNA
    testdf1.drop(testdf1[(testdf1['latitude'] ==-88) & (testdf1['longitude']==-88)].index) #drop -88s

    gdf1 = geopandas.GeoDataFrame(testdf1, geometry=geopandas.points_from_xy(testdf1['longitude'], testdf1['latitude']))

    bruv_map = folium.Map(location=[41.9,-97.3], tiles = "Stamen Terrain",zoom_start = 4)

    loc = 'BRUV Visual Map'
    title_html = '''
             <h3 align="center" style="font-size:14px"><b>{}</b></h3>
             '''.format(loc)

    legend_html = """
{% macro html(this, kwargs) %}
<div style="
    position: fixed;
    bottom: 40px;
    left: 10px;
    width: 250px;
    height: 100px;
    z-index:9999;
    font-size:12px;
    ">
    <b><u>LEGEND:</u></b>
    <p><a style="color:#000000;font-size:90%;margin-left:20px;">◼</a>&emsp;BRUV Location</p>

</div>
<div style="
    position: fixed;
    bottom: 40px;
    left: 10px;
    width: 150px;
    height: 100px;
    z-index:9998;
    font-size:12px;
    background-color: #ffffff;
    filter: blur(5px);
    -webkit-filter: blur(5px);
    opacity: 0.7;
    ">
</div>
{% endmacro %}
"""
    legend = branca.element.MacroElement()
    legend._template = branca.element.Template(legend_html)

    for i in range(0,len(gdf1)):
        folium.Marker(
            location=[gdf1.iloc[i]['latitude'], gdf1.iloc[i]['longitude']],
            popup=[gdf1.iloc[i]['estuaryname'],(gdf1.iloc[i]['latitude'], gdf1.iloc[i]['longitude'])],
            icon=folium.Icon(color='black',icon_color='#FFFF00'),
        ).add_to(map)

    bruv_map.get_root().add_child(legend)
    bruv_map.get_root().html.add_child(folium.Element(title_html))

    return bruv_map


