from arcgis.features import GeoAccessor
from arcgis.geometry import LengthUnits
from arcgis import geometry
from arcgis import GIS
import os
import zipfile
import pathlib
import glob
# Distance: https://developers.arcgis.com/rest/services-reference/enterprise/distance.htm
# Unit: https://resources.arcgis.com/en/help/arcobjects-cpp/componenthelp/index.html#/esriSRUnitType_Constants/000w00000042000000/
# 9001 for international meter
def distance_between(x1, x2, wkid=4326):
    ref = geometry.SpatialReference({'latestWkid': wkid, 'wkid': wkid})
    x1.spatialReference = ref
    x2.spatialReference = ref
    distance = geometry.distance(
        ref, 
        x1, 
        x2, 
        distance_unit=9102
    )
    return distance

def read_shapefile(path):
    fname = pathlib.Path(path).name.replace(".zip","")
    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(fname)
    shp_dir = glob.glob(
        os.path.join(
            os.getcwd(),
            fname,
            "*.shp"
        )
    )
    if len(shp_dir) > 0: 
        return GeoAccessor.from_featureclass(shp_dir[0])
    else:
        print("No shp found in folder")
gis = GIS()
path_to_zipfile = "Stations_Map.zip"
df = read_shapefile(path_to_zipfile)
x1 = df['SHAPE'].iloc[0]
x2 = df['SHAPE'].iloc[1]
distance = distance_between(x1, x2)