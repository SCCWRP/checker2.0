import folium
import geopandas
import pandas as pd
import json
import os
from shapely.geometry import Point
from matplotlib import colors
import branca
from folium import plugins
from .sav import sav
from inspect import currentframe
from flask import current_app
from .functions import checkData, get_badrows

def sav_visual_map(all_dfs, spatialtable):
    testdf = all_dfs.get(spatialtable)

    testdf1=testdf.dropna(subset=['savbedc1latitude', 'savbedc1longitude']) #dropNA
    testdf1.drop(testdf1[(testdf1['savbedc1latitude'] ==-88) & (testdf1['savbedc1longitude']==-88)].index)

    testdf2=testdf.dropna(subset=['savbedc2latitude', 'savbedc2longitude']) #dropNA)
    testdf2.drop(testdf2[(testdf2['savbedc2latitude'] ==-88) & (testdf2['savbedc2longitude']==-88)].index)

    testdf3=testdf.dropna(subset=['savbedc3latitude', 'savbedc3longitude']) #dropNA)
    testdf3.drop(testdf3[(testdf3['savbedc3latitude'] ==-88) & (testdf3['savbedc3longitude']==-88)].index)

    testdf4=testdf.dropna(subset=['savbedc4latitude', 'savbedc4longitude']) #dropNA)
    testdf4.drop(testdf4[(testdf4['savbedc4latitude'] ==-88) & (testdf4['savbedc4longitude']==-88)].index)

    testdf5=testdf.dropna(subset=['savbedcenterlatitude', 'savbedcenterlongitude']) #dropNA)
    testdf5.drop(testdf5[(testdf5['savbedcenterlatitude'] ==-88) & (testdf5['savbedcenterlongitude']==-88)].index)

    testdf6=testdf.dropna(subset=['transectbeginlatitude', 'transectbeginlongitude']) #dropNA)
    testdf6.drop(testdf6[(testdf6['transectbeginlatitude'] ==-88) & (testdf6['transectbeginlongitude']==-88)].index)

    testdf7=testdf.dropna(subset=['transectendlatitude', 'transectendlongitude']) #dropNA)
    testdf7.drop(testdf7[(testdf7['transectendlatitude'] ==-88) & (testdf7['transectendlongitude']==-88)].index)

    #geodataframes for all 7 points:

    gdf1 = geopandas.GeoDataFrame(testdf1, geometry=geopandas.points_from_xy(testdf1['savbedc1longitude'], testdf1['savbedc1latitude']))

    gdf2 = geopandas.GeoDataFrame(testdf2, geometry=geopandas.points_from_xy(testdf2['savbedc2longitude'], testdf2['savbedc2latitude']))

    gdf3 = geopandas.GeoDataFrame(testdf3, geometry=geopandas.points_from_xy(testdf3['savbedc3longitude'], testdf3['savbedc3latitude']))

    gdf4 = geopandas.GeoDataFrame(testdf4, geometry=geopandas.points_from_xy(testdf4['savbedc4longitude'], testdf4['savbedc4latitude']))

    gdf5 = geopandas.GeoDataFrame(testdf5, geometry=geopandas.points_from_xy(testdf5['savbedcenterlatitude'], testdf5['savbedcenterlongitude']))

    gdf6 = geopandas.GeoDataFrame(testdf6, geometry=geopandas.points_from_xy(testdf6['transectbeginlatitude'], testdf6['transectbeginlongitude']))

    gdf7 = geopandas.GeoDataFrame(testdf7, geometry=geopandas.points_from_xy(testdf7['transectendlatitude'], testdf7['transectendlongitude']))

    #Map: Adding all Locations to The Map

    sav_map = folium.Map(location=[41.9,-97.3], tiles = "Stamen Terrain",zoom_start = 4)
    loc = 'SAV Visual Map'
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
    height: 200px;
    z-index:9999;
    font-size:12px;
    ">
    <b><u>LEGEND:</u></b>
    <p><a style="color:#000000;font-size:90%;margin-left:20px;">◼</a>&emsp;Savbedc1</p>
    <p><a style="color:#FE4E02;font-size:90%;margin-left:20px;">◼</a>&emsp;Savbedc2</p>
    <p><a style="color:#0224FE;font-size:90%;margin-left:20px;">◼</a>&emsp;Savbedc3</p>
    <p><a style="color:#E74641;font-size:90%;margin-left:20px;">◼</a>&emsp;Savbedc4</p>
    <p><a style="color:#14B41E;font-size:90%;margin-left:20px;">◼</a>&emsp;SavbedCenter</p>
    <p><a style="color:#A1A1A1;font-size:90%;margin-left:20px;">◼</a>&emsp;Transectbegin</p>
    <p><a style="color:#920402;font-size:90%;margin-left:20px;">◼</a>&emsp;Transectend</p>
</div>
<div style="
    position: fixed;
    bottom: 40px;
    left: 10px;
    width: 150px;
    height: 200px;
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
            location=[gdf1.iloc[i]['savbedc1latitude'], gdf1.iloc[i]['savbedc1longitude']],
            popup=[gdf1.iloc[i]['estuaryname'],(gdf1.iloc[i]['savbedc1latitude'], gdf1.iloc[i]['savbedc1longitude'])],
            icon=folium.Icon(color='black',icon_color='#FFFF00'),
        ).add_to(sav_map)

    for i in range(0,len(gdf2)):
        folium.Marker(
            location=[gdf2.iloc[i]['savbedc2latitude'], gdf2.iloc[i]['savbedc2longitude']],
            popup=[gdf2.iloc[i]['estuaryname'],(gdf2.iloc[i]['savbedc2latitude'], gdf2.iloc[i]['savbedc2longitude'])],
            icon=folium.Icon(color='orange',icon_color='#000000')
        ).add_to(sav_map)

    for i in range(0,len(gdf3)):
        folium.Marker(
            location=[gdf3.iloc[i]['savbedc3latitude'], gdf3.iloc[i]['savbedc3longitude']],
            popup=[gdf3.iloc[i]['estuaryname'],(gdf3.iloc[i]['savbedc3latitude'], gdf3.iloc[i]['savbedc3longitude'])],
            icon=folium.Icon(color='darkblue',icon_color='#FFFF00')
        ).add_to(sav_map)

    for i in range(0,len(gdf4)):
        folium.Marker(
            location=[gdf4.iloc[i]['savbedc4latitude'], gdf4.iloc[i]['savbedc4longitude']],
            popup=[gdf4.iloc[i]['estuaryname'],(gdf4.iloc[i]['savbedc4latitude'], gdf4.iloc[i]['savbedc4longitude'])],
            icon=folium.Icon(color='lightred',icon_color='#000000')
        ).add_to(sav_map)

    for i in range(0,len(gdf5)):
        folium.Marker(
            location=[gdf5.iloc[i]['savbedcenterlatitude'], gdf5.iloc[i]['savbedcenterlongitude']],
            popup=[gdf5.iloc[i]['estuaryname'],(gdf5.iloc[i]['savbedcenterlatitude'], gdf5.iloc[i]['savbedcenterlongitude'])],
            icon=folium.Icon(color='green',icon_color='#000000')
        ).add_to(sav_map)

    for i in range(0,len(gdf6)):
        folium.Marker(
            location=[gdf6.iloc[i]['transectbeginlatitude'], gdf6.iloc[i]['transectbeginlongitude']],
            popup=[gdf6.iloc[i]['estuaryname'],(gdf6.iloc[i]['transectbeginlatitude'], gdf6.iloc[i]['transectbeginlongitude'])],
            icon=folium.Icon(color='lightgray',icon_color='#FFFF00')
        ).add_to(sav_map)

    for i in range(0,len(gdf7)):
        folium.Marker(
            location=[gdf7.iloc[i]['transectendlatitude'], gdf7.iloc[i]['transectendlongitude']],
            popup=[gdf7.iloc[i]['estuaryname'],(gdf7.iloc[i]['transectendlatitude'], gdf7.iloc[i]['transectendlongitude'])],
            icon=folium.Icon(color='darkred',icon_color='#FFFF00')
        ).add_to(sav_map)

    sav_map.get_root().add_child(legend)

    sav_map.get_root().html.add_child(folium.Element(title_html))

    return sav_map


