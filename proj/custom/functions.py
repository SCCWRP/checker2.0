import json, binascii, os
from pandas import isnull, DataFrame, to_datetime, read_sql
import math
import numpy as np
from inspect import currentframe
from arcgis.geometry.filters import within, contains
from arcgis.geometry import Point, Polyline, Polygon, Geometry
from arcgis.geometry import lengths, areas_and_lengths, project
from arcgis.geometry.functions import intersect as arc_geometry_intersect
import pandas as pd
from flask import current_app
import json


# for trawl distance function
from shapely.geometry import Point as shapelyPoint
from shapely.geometry import Polygon as shapelyPolygon
from shapely.geometry import LineString as shapelyLineString
from shapely import wkb
from pyproj import CRS, Transformer

def checkData(tablename, badrows, badcolumn, error_type, error_message = "Error", is_core_error = False, errors_list = [], q = None, **kwargs):
    
    # See comments on the get_badrows function
    # doesnt have to be used but it makes it more convenient to plug in a check
    # that function can be used to get the badrows argument that would be used in this function
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "is_core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "is_core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        



# checkLogic() returns indices of rows with logic errors
def checkLogic(df1, df2, cols: list, error_type = "Logic Error", df1_name = "", df2_name = "", row_index_col = 'tmp_row'):
    ''' each record in df1 must have a corresponding record in df2'''
    print("checkLogic")
    assert \
    set([x.lower() for x in cols]).issubset(set(df1.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df1_name, ','.join(df1.columns)
    )

    assert \
    set([x.lower() for x in cols]).issubset(set(df2.columns)), \
    "({}) not in columns of {} ({})" \
    .format(
        ','.join([x.lower() for x in cols]), df2_name, ','.join(df2.columns)
    )

    # 'Kristin wrote this code in ancient times.'
    lcols = [x.lower() for x in cols] # lowercase cols
    tmp_missing_val = 'missing_value'
    badrowsdf = df1[
        ~df1[lcols].fillna(tmp_missing_val).isin(df2[lcols].fillna(tmp_missing_val).to_dict(orient='list')).all(axis=1)
    ]
    
    badrows = badrowsdf[row_index_col].tolist() if row_index_col != 'index' else badrowsdf.index.tolist()

    print("end checkLogic")

    return {
        "badrows": badrows,
        "badcolumn": ','.join(cols),
        "error_type": "Logic Error",
        "error_message": f"""Each record in {df1_name} must have a matching record in {df2_name}. Records are matched on {','.join(cols)}"""
    }


def mismatch(df1, df2, mergecols = None, left_mergecols = None, right_mergecols = None, row_identifier = 'tmp_row'):
    # gets rows in df1 that are not in df2
    # row identifier column is tmp_row by default

    # If the first dataframe is empty, then there can be no badrows
    if df1.empty:
        return []

    # if second dataframe is empty, all rows in df1 are mismatched
    if df2.empty:
        return df1[row_identifier].tolist() if row_identifier != 'index' else df1.index.tolist()
    # Hey, you never know...
    assert not '_present_' in df1.columns, 'For some reason, the reserved column name _present_ is in columns of df1'
    assert not '_present_' in df2.columns, 'For some reason, the reserved column name _present_ is in columns of df2'

    if mergecols is not None:
        assert set(mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
        assert set(mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(mergecols)} is not a subset of the columns of the dataframe """
    
        # if datatypes dont match, then perform the type coercion. Otherwise merge normally
        # This should solve github issue #19 
        # # (issue #19 in the bight23checker repository at least, and it should prevent it in subsequent versions which are created after this one)
         
        if df1[mergecols].dtypes.tolist() == df2[mergecols].dtypes.tolist():
            tmp = df1 \
                .merge(
                    df2.assign(_present_='yes'),
                    on = mergecols, 
                    how = 'left',
                    suffixes = ('','_df2')
                )
        else:
            tmp = df1.astype(str) \
                .merge(
                    df2.astype(str).assign(_present_='yes'),
                    on = mergecols, 
                    how = 'left',
                    suffixes = ('','_df2')
                )
        

    elif (right_mergecols is not None) and (left_mergecols is not None):
        assert set(left_mergecols).issubset(set(df1.columns)), f"""In mismatch function - {','.join(left_mergecols)} is not a subset of the columns of the dataframe of the first argument"""
        assert set(right_mergecols).issubset(set(df2.columns)), f"""In mismatch function - {','.join(right_mergecols)} is not a subset of the columns of the dataframe of the second argument"""
        
        if df1[left_mergecols].dtypes.tolist() == df2[right_mergecols].dtypes.tolist():
            tmp = df1 \
                .merge(
                    df2.assign(_present_='yes'),
                    left_on = left_mergecols, 
                    right_on = right_mergecols, 
                    how = 'left',
                    suffixes = ('','_df2')
                )
        else:
            tmp = df1.astype(str) \
                .merge(
                    df2.astype(str).assign(_present_='yes'),
                    left_on = left_mergecols, 
                    right_on = right_mergecols, 
                    how = 'left',
                    suffixes = ('','_df2')
                )

    else:
        raise Exception("In mismatch function - improper use of function - No merging columns are defined")

    if not tmp.empty:
        badrows = tmp[pd.isnull(tmp._present_)][row_identifier].tolist() \
            if row_identifier not in (None, 'index') \
            else tmp[pd.isnull(tmp._present_)].index.tolist()
    else:
        badrows = []

    assert \
        all(isinstance(item, int) or (isinstance(item, str) and item.isdigit()) for item in badrows), \
        "In mismatch function - Not all items in 'badrows' are integers or strings representing integers"

    badrows = [int(x) for x in badrows]
    return badrows


def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    m = 6367000 * c
    return m

def check_time(starttime, endtime):
    df_over = to_datetime(starttime)
    df_start = to_datetime(endtime)
    times = (df_over - df_start).astype('timedelta64[m]')
    return abs(times)

def check_distance(df,start_lat,end_lat,start_lon,end_lon):
    distance = []
    ct = math.pi/180.0 #conversion factor
    for index in df.index:
        dis = math.acos(math.sin(start_lat[index] * ct) * math.sin(end_lat[index] * ct) + math.cos(start_lat[index] * ct)*math.cos(end_lat[index] * ct)*math.cos((end_lon[index] * ct)-(start_lon[index] * ct)))*6371000
        distance.append(dis)
    return distance

# courtesy of chatgpt - for the trawl line distance check (100m distance allowed, 200m fior channel islands region)
def calculate_distance(row):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)  # Transformer object from WGS84 to pseudo-Mercator EPSG:3857

    # Define point and line
    point = shapelyPoint(transformer.transform(row['targetlongitude'], row['targetlatitude']))
    line = shapelyLineString([(transformer.transform(row['startlongitude'], row['startlatitude'])), (transformer.transform(row['endlongitude'], row['endlatitude']))])
    
    # Calculate distance in meters
    distance = point.distance(line)
    
    return distance  # Distance in meters

def multivalue_lookup_check(df, field, listname, listfield, dbconnection, displayfieldname = None, sep=','):
    """
    Checks a column of a dataframe against a column in a lookup list. Specifically if the column may have multiple values.
    The default is that the user enters multiple values separated by a comma, although the function may take other characters as separators
    
    Parameters:
    df               : The user's dataframe
    field            : The field name of the user's submitted dataframe
    listname         : The Lookup list name (for example lu_resqualcode)
    listfield        : The field of the lookup list table that we are checking against
    displayfieldname : What the user will see in the error report - defaults to the field argument 
                       it should still be a column in the dataframe, but with different capitalization

    Returns a dictionary of arguments to pass to the checkData function
    """

    # default the displayfieldname to the "field" argument
    displayfieldname = displayfieldname if displayfieldname else field

    # displayfieldname should still be a column of the dataframe, but just typically camelcased
    assert displayfieldname.lower() in df.columns, f"the displayfieldname {displayfieldname} was not found in the columns of the dataframe, even when it was lowercased"

    assert field in df.columns, f"In {str(currentframe().f_code.co_name)} (value against multiple values check) - {field} not in the columns of the dataframe"
    lookupvals = set(read_sql(f'''SELECT DISTINCT "{listfield}" FROM "{listname}";''', dbconnection)[listfield].tolist())

    if not 'tmp_row' in df.columns:
        df['tmp_row'] = df.index

    # hard to explain what this is doing through a code comment
    badrows = df[df[field].apply(lambda values: not set([val.strip() for val in str(values).split(sep)]).issubset(lookupvals) )].tmp_row.tolist()
    args = {
        "badrows": badrows,
        "badcolumn": displayfieldname,
        "error_type": "Lookup Error",
        "error_message": f"""One of the values here is not in the lookup list <a target = "_blank" href=/{current_app.script_root}/scraper?action=help&layer={listname}>{listname}</a>"""
    }

    return args


def check_strata_grab(grab, strata_lookup, field_assignment_table):

    strata_lookup['SHAPE'] = strata_lookup['shape'].apply( lambda x:  wkb.loads(binascii.unhexlify(x)) )

    # Get the columns stratum, region from stations_grab_final, merged on stationid.
    # We need these columns to look up for the polygon the stations are supposed to be in
    grab = pd.merge(
        grab, 
        field_assignment_table[['stationid','stratum','region']].drop_duplicates(), 
        how='left', 
        on=['stationid']
    ).merge(
        # tack on that region exists column because it will show up as True if it exists, NULL otherwise
        strata_lookup.assign(region_exists_in_featurelayer = True),
        on = ['region','stratum'],
        how = 'left'
    )

    # We essentially make it a critical error when the field assignment doesnt match the feature layer
    # The reason for this is that it essentially is a server side error rather than a user error
    # If they are submitting data, and we havent worked that out, that is a big problem that needs to be addressed, so we will raise a critical
    # If we are on top of it, this should never happen during the entire bight 2023 cycle
    print("Before assertion")
    # assert \
    #     pd.notnull(grab.region_exists_in_featurelayer).all(), \
    #     "There are region/stratum combinations in the field assignment table that do not match the bight region feature layer"
    if not pd.notnull(grab.region_exists_in_featurelayer).all():
        print("THERE IS A PROBLEM")
        print("There are region/stratum combinations in the field assignment table that do not match the bight region feature layer")
        raise Exception("There are region/stratum combinations in the field assignment table that do not match the bight region feature layer")
    


    print("grab after merge")
    print(grab)
    # Make the points based on long, lat columns of grab
    grab['grabpoint'] = grab.apply(
        lambda row: shapelyPoint(row['longitude'], row['latitude']), axis=1
    )

    # Now we check if the points are in associated polygon or not. Assign True if they are in
    print("Now we check if the points are in associated polygon or not. Assign True if they are in")
    print("strata_lookup")
    print(strata_lookup)
    grab['is_station_in_strata'] = grab.apply(
        lambda row: row.SHAPE.contains(row['grabpoint']) if row.SHAPE else False, axis=1
    )

    # Convert shapely Point to ArcGIS Point and assign it to the SHAPE column
    grab['SHAPE'] = grab.apply(lambda row: Point({"x": row['longitude'], "y": row['latitude'], "spatialReference": {"wkid": 4326}}), axis=1)

    # Drop the temporary grabpoint column
    grab.drop(['grabpoint'], axis='columns', inplace=True)

    # Now we get the bad rows
    bad_df = grab.assign(tmp_row=grab.index).query("is_station_in_strata == False")
    return bad_df

def check_strata_trawl(trawl, strata_lookup, field_assignment_table):

    strata_lookup['SHAPE'] = strata_lookup['shape'].apply( lambda x:  wkb.loads(binascii.unhexlify(x)) )
    
    # Get the columns stratum, region from stations_grab_final, merged on stationid.
    # We need these columns to look up for the polygon the stations are supposed to be in
    trawl = pd.merge(
        trawl, 
        field_assignment_table.filter(items=['stationid','stratum','region']).drop_duplicates(), 
        how='left', 
        on=['stationid']
    ).merge(
        # tack on that region exists column because it will show up as True if it exists, NULL otherwise
        strata_lookup.assign(region_exists_in_featurelayer = True),
        on = ['region','stratum'],
        how = 'left'
    )
    print("trawl")
    print(trawl)

    # We essentially make it a critical error when the field assignment doesnt match the feature layer
    # The reason for this is that it essentially is a server side error rather than a user error
    # If they are submitting data, and we havent worked that out, that is a big problem that needs to be addressed, so we will raise a critical
    # If we are on top of it, this should never happen during the entire bight 2023 cycle
    print("Before assertion")
    # assert \
    #     pd.notnull(trawl.region_exists_in_featurelayer).all(), \
    #     "There are region/stratum combinations in the field assignment table that do not match the bight region feature layer"
    if not pd.notnull(trawl.region_exists_in_featurelayer).all():
        print("THERE IS A PROBLEM")
        print("There are region/stratum combinations in the field assignment table that do not match the bight region feature layer")
        raise Exception("There are region/stratum combinations in the field assignment table that do not match the bight region feature layer")
    
    print("After assertion")


    # Make the points based on long, lat columns of grab
    trawl['trawl_line'] = trawl.apply(
        lambda row: shapelyLineString([
            (row['startlongitude'], row['startlatitude']), (row['endlongitude'], row['endlatitude'])
        ]),
        axis=1
    )

    # make a column of shapely polygons - this will be the bight region polygon to check for intersection with trawl line
    # We already asserted that there will be no missing values in the SHAPE column
    
    
    # Now we check if the LineStrings intersect associated polygon or not. Assign True if they do
    trawl['is_station_in_strata'] = trawl.apply(
        lambda row: row.SHAPE.intersects(row['trawl_line']) if row.SHAPE else False, axis=1
    )

    # Need to ensure the SHAPE column is the trawl line as an arcgis geometry Polyline object
    # This is because there is a function that exports a spatial dataframe to a json which expects this column, as an "ArcGIS API for Python type of object"
    trawl['SHAPE'] = trawl.trawl_line.apply(lambda line: Polyline({"paths": [list(line.coords)], "spatialReference": {"wkid": 4326}}) )

    # We also should drop the temp columns created in this function, since we dont want them included in the geojson that will be put on the map
    # these objects are also not json serializable so it makes it difficult, so its better we just drop the columns
    trawl.drop(['region_polygon','trawl_line'], axis = 'columns', inplace = True, errors = 'ignore')

    print('in the trawl strata check - trawl dataframe')

    print(trawl)

    # Now we get the bad rows
    bad_df = trawl.assign(tmp_row=trawl.index).query("is_station_in_strata == False")    
    return bad_df

def export_sdf_to_json(path, sdf):
    print('path')
    print(path)
    print('sdf')
    print(sdf)
    if not sdf.empty:
        if "paths" in sdf['SHAPE'].iloc[0].keys():
            # data = [
            #     {
            #         "type":"polyline",
            #         "paths" : item.get('paths')[0]
            #     }
            #     for item in sdf['SHAPE']
            # ]
            data = [
                {
                    "type" : "Feature",
                    "geometry" : {
                        "type":"polyline",
                        "paths" : row.SHAPE.get('paths')
                    },
                    "properties" : {
                        k:str(v) for k,v in row.items() if k not in ('strata_polygon', 'SHAPE')
                    }
                }

                for _, row in sdf.iterrows()
            ]        
        elif "rings" in sdf['SHAPE'].iloc[0].keys():

            # data = [
            #     {
            #         "type":"polygon",
            #         "rings" : item.get('rings')[0]
            #     }
            #     for item in sdf['SHAPE']
            # ]        
            data = [
                {
                    "type" : "Feature",
                    "geometry" : {
                        "type":"polygon",
                        "rings" : row.SHAPE.get('rings')
                    },
                    "properties" : {
                        k:str(v) for k,v in row.items() if k not in ('strata_polygon', 'SHAPE')
                    }
                }

                for _, row in sdf.iterrows()
            ]        
        else:
            # data = [
            #     {
            #         "type":"point",
            #         "longitude": item["x"],
            #         "latitude": item["y"]
            #     }
            #     for item in sdf.get("SHAPE").tolist()
            # ]
            data = [
                {
                    "type" : "Feature",
                    "geometry" : {
                        "type":"point",
                        "longitude": row.SHAPE.get("x"),
                        "latitude": row.SHAPE.get("y")
                    },
                    "properties" : {
                        k:str(v) for k,v in row.items() if k not in ('strata_polygon', 'SHAPE')
                    }
                }

                for _, row in sdf.iterrows()
            ]        
    else:
        data = []    
    with open(path, "w", encoding="utf-8") as geojson_file:
        json.dump(data, geojson_file)

# For benthic, we probably have to tack on a column that just contains values that say "Infauna" and then use that as the parameter column
# For chemistry, we have to tack on the analyteclass column from lu_analytes and then use that as the parameter column
def sample_assignment_check(eng, df, parameter_column, row_index_col = 'tmp_row', stationid_column = 'stationid', dataframe_agency_column = 'lab', assignment_agency_column = 'assigned_agency', assignment_table = 'vw_sample_assignment', excepted_params = []):
    '''
        Simply Returns the "badrows" list of indices where the parameter and lab doesnt match the assignment table
    '''
    # No SQL injection
    assignment_table = str(assignment_table).replace(';','').replace('"','').replace("'","")
    stationid_column = str(stationid_column).replace(';','').replace('"','').replace("'","")
    dataframe_agency_column = str(dataframe_agency_column).replace(';','').replace('"','').replace("'","")
    assignment_agency_column = str(assignment_agency_column).replace(';','').replace('"','').replace("'","")
    parameter_column = str(parameter_column).replace(';','').replace('"','').replace("'","")
    
    assignment = pd.read_sql(
        f'''SELECT DISTINCT {stationid_column}, parameter AS {parameter_column}, {assignment_agency_column} AS {dataframe_agency_column}, 'yes' AS present FROM "{assignment_table}"; ''', 
        eng
    )

    df = df[~df[parameter_column].isin(excepted_params)].merge(assignment, on = [stationid_column, parameter_column, dataframe_agency_column], how = 'left')


    badrows = df[(df.present.isnull()) & (df[stationid_column] != '0000')][row_index_col].tolist() if row_index_col != 'index' else df[(df.present.isnull() ) & (df[stationid_column] != '0000')].index.tolist()

    return badrows



# Check Logic of Grab/Trawl Numbers and only return the badrows
def check_samplenumber_sequence(df_to_check, col, samplenumbercol):
    assert col in df_to_check.columns, f"{col} not found in columns of the dataframe passed into check_samplenumber_sequence"
    assert 'stationid' in df_to_check.columns, "'stationid' not found in columns of the dataframe passed into check_samplenumber_sequence"
    assert 'sampledate' in df_to_check.columns, "'sampledate' not found in columns of the dataframe passed into check_samplenumber_sequence"

    df_to_check[col] = pd.to_datetime(df_to_check[col], format='%H:%M:%S').dt.time

    df_to_check = df_to_check.sort_values(['stationid', 'sampledate', col])

    trawl_grouped = df_to_check.groupby(['stationid', 'sampledate']).apply(lambda grp: grp[samplenumbercol].is_monotonic_increasing).reset_index()
    trawl_grouped.columns = ['stationid', 'sampledate', 'correct_order']

    badrows = df_to_check.merge(
            trawl_grouped[trawl_grouped['correct_order'] == False],
            on = ['stationid', 'sampledate'],
            how = 'inner'
        ) \
        .tmp_row \
        .tolist()
    return badrows