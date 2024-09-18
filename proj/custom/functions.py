import re
from pandas import to_datetime, read_sql
import math
import numpy as np
import pandas as pd
from flask import current_app

from shapely.geometry import Point as shapelyPoint
from shapely.geometry import LineString as shapelyLineString
from pyproj import Transformer


def checkData(tablename, badrows, badcolumn, error_type, error_message = "Error", is_core_error = False, errors_list = [], q = None, **kwargs):
    
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


def check_time_format(df, column, time_format_regex=r'^\d{2}:\d{2}:\d{2}$', row_identifier = 'tmp_row'):
    """
    Checks columns of a dataframe for a 24 hour time format (HH:MM:SS) and ensures the values are logical.

    Parameters:
    df                : The user's dataframe
    column            : The column name of the dataframe to check
    time_format_regex : The regex pattern to match the time format (default is HH:MM:SS)
    row_identifier    : The column name to use as row identifier (default is 'tmp_row')

    Returns a list of indices of rows with invalid time format
    """
    assert column in df.columns, f"The column {column} was not found in the dataframe"

    if not 'tmp_row' in df.columns:
        df['tmp_row'] = df.index

    def is_valid_time_format(time_str):
        if not re.match(time_format_regex, time_str):
            return False
        hours, minutes, seconds = map(int, time_str.split(':'))
        return 0 <= hours < 24 and 0 <= minutes < 60 and 0 <= seconds < 60

    badrows = df[~df[column].astype(str).apply(is_valid_time_format)][row_identifier].tolist()
    
    return badrows
