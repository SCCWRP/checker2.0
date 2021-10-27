import os
import geopandas
import json
from shapely.geometry import Point
import folium
import os, time, json
import pandas as pd
from folium import plugins
import branca
from .vegetation import vegetation
from inspect import currentframe
from flask import current_app
from .functions import checkData, get_badrows

def veg_visual_map(all_dfs, spatialtable):
    """
    takes pandas dataframe, converts to geopandas GeoDataFrame
    """
    testdf = all_dfs.get(spatialtable)
    
    testdf1=testdf.dropna(subset=['transectbeginlatitude', 'transectbeginlongitude']) #dropNA
    testdf1.drop(testdf1[(testdf1['transectbeginlatitude'] ==-88) & (testdf1['transectbeginlongitude']==-88)].index) #get rid of -88
    
    testdf2=testdf.dropna(subset=['transectendlatitude', 'transectendlongitude']) #dropNA
    testdf2.drop(testdf2[(testdf2['transectendlatitude'] ==-88) & (testdf2['transectendlongitude']==-88)].index) #get rid of -88
    
    gdf1 = geopandas.GeoDataFrame(testdf1, geometry=geopandas.points_from_xy(testdf1['transectbeginlongitude'], testdf1['transectbeginlatitude']))
    
    gdf2 = geopandas.GeoDataFrame(testdf2, geometry=geopandas.points_from_xy(testdf2['transectendlongitude'], testdf2['transectendlatitude']))
    
    veg_map = folium.Map(location=[41.9,-97.3], tiles = "Stamen Terrain",zoom_start = 4)
    
    loc = 'Vegetation Visual Map'
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
        <p><a style="color:#000000;font-size:90%;margin-left:20px;">◼</a>&emsp;Transect begin</p>
        <p><a style="color:#FE4E02;font-size:90%;margin-left:20px;">◼</a>&emsp;Transect end</p>
    
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
          location=[gdf1.iloc[i]['transectbeginlatitude'], gdf1.iloc[i]['transectbeginlongitude']],
          popup=[gdf1.iloc[i]['estuaryname'],(gdf1.iloc[i]['transectbeginlatitude'], gdf1.iloc[i]['transectbeginlongitude'])],
          icon=folium.Icon(color='black',icon_color='#FFFF00'),
       ).add_to(veg_map)
    
    for i in range(0,len(gdf2)):
       folium.Marker(
          location=[gdf2.iloc[i]['transectendlatitude'], gdf2.iloc[i]['transectendlongitude']],
          popup=[gdf2.iloc[i]['estuaryname'],(gdf2.iloc[i]['transectendlatitude'], gdf2.iloc[i]['transectendlongitude'])],
          icon=folium.Icon(color='orange',icon_color='#000000')
       ).add_to(veg_map)
    veg_map.get_root().add_child(legend)
    veg_map.get_root().html.add_child(folium.Element(title_html))
   
    
    
    return veg_map
        
    
