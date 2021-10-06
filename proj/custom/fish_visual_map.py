import folium
import geopandas
import pandas as pd
import json
import os
from shapely.geometry import Point
from matplotlib import colors
import branca
from .fishseines import fishseines
from inspect import currentframe
from flask import current_app
from .functions import checkData, get_badrows

def fish_visual_map(filepath):
    testdf = pd.excel_file(filepath,sheet_name="fishmeta")
    
    testdf1=testdf.dropna(subset=['netbeginlatitude', 'netbeginlongitude']) #dropNA
    testdf1.drop(testdf1[(testdf1['netbeginlatitude'] ==-88) & (testdf1['netbeginlongitude']==-88)].index)
    
    testdf2=testdf.dropna(subset=['netendlatitude', 'netendlongitude']) #dropNA
    testdf2.drop(testdf2[(testdf2['netendlatitude'] ==-88) & (testdf2['netendlongitude']==-88)].index)
    
    gdf1 = geopandas.GeoDataFrame(testdf1, geometry=geopandas.points_from_xy(testdf1['netbeginlongitude'], testdf1['netbeginlatitude']))

    gdf2 = geopandas.GeoDataFrame(testdf2, geometry=geopandas.points_from_xy(testdf2['netendlongitude'], testdf2['netendlatitude']))
    
    fish_map = folium.Map(location=[41.9,-97.3], tiles = "Stamen Terrain",zoom_start = 4)
    
    loc = 'Fish Visual Map'
    title_html = '''
             <h3 align="center" style="font-size:16px"><b>{}</b></h3>
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
    <p><a style="color:#000000;font-size:90%;margin-left:20px;">◼</a>&emsp;Net begin</p>
    <p><a style="color:#FE4E02;font-size:90%;margin-left:20px;">◼</a>&emsp;Net end</p>

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
      location=[gdf1.iloc[i]['netbeginlatitude'], gdf1.iloc[i]['netbeginlongitude']],
      popup=[gdf1.iloc[i]['estuaryname'],(gdf1.iloc[i]['netbeginlatitude'], gdf1.iloc[i]['netbeginlongitude'])],
      icon=folium.Icon(color='black',icon_color='#FFFF00'),
   ).add_to(fish_map)

for i in range(0,len(gdf2)):
   folium.Marker(
      location=[gdf2.iloc[i]['netendlatitude'], gdf2.iloc[i]['netendlongitude']],
      popup=[gdf2.iloc[i]['estuaryname'],(gdf2.iloc[i]['netendlatitude'], gdf2.iloc[i]['netendlongitude'])],
      icon=folium.Icon(color='orange',icon_color='#000000')
   ).add_to(fish_map)
fish_map.get_root().add_child(legend)
fish_map.get_root().html.add_child(folium.Element(title_html))


return fish_map