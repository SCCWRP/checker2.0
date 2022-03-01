from math import radians, cos, sin, asin, sqrt
from xml.etree import ElementTree as et
import requests

def correct_depth_ctd(loggerc):
    #convert pressure variables from mH2O to cmH2O
    loggerc['pressure_cmh2o'] = loggerc['pressure_mh2o']*(10**2)
    #retrieve from buoy data
    #loggerc['baropressure_mbar'] = 1 # tmp value ---- this is a col in the df
    #convert baropressure_mbar to cmH2O
    #convert using 1 mbar = 1.019272 cmH2O
    loggerc['baropressure_cmh2o'] = loggerc['baropressure_mbar']*(1.019272)
    #corrected pressure in cmH2O by computing difference
    loggerc['pressure_corr'] = loggerc['pressure_cmh2o'] - loggerc['baropressure_cmh2o']
    #water column (wc, in height m)
    #multiplied by 0.01 to convert from cm to m
    print("did it get here")
    loggerc['wc_m'] = (9806.65*(loggerc['pressure_corr'])/(rho*g))*0.01
    return(loggerc)

#######################
def dist(lat1, long1, lat2, long2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    print(lat1, long1, lat2, long2)
    print(f"type: {type(lat1)}, {type(long1)}, {type(lat2)}, {type(long2)}")
    lat1, long1, lat2, long2 = map(radians, [lat1, long1, float(lat2), float(long2)])
    print(lat1, long1, lat2, long2)
    print(f"type: {type(lat1)}, {type(long1)}, {type(lat2)}, {type(long2)}")
    # haversine formula 
    dlon = long2 - long1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return(km)

def find_nearest(lat, long):
    distances = df.apply(
        lambda row: dist(lat, long, row['latitude'], row['longitude']), 
        axis=1)
    tmp = min(distances)
    print(f'tmp: {tmp}')
    return(df.loc[distances.idxmin(), 'stationid'])

#######################

def parse_xml(eroot):
    attr = eroot.attrib
    for root_element in eroot.findall('Station'):
        xml_data = attr.copy()
        xml_data.update(root_element.attrib)
        print(xml_data)
        xml_data['stationid'] = root_element.find('id').text
        xml_data['name'] = root_element.find('name').text
        xml_data['latitude'] = root_element.find('lat').text
        xml_data['longitude'] = root_element.find('lng').text
        xml_data['affiliations'] = root_element.find('affiliations').text
        xml_data['products'] = root_element.find('products').text
        xml_data['disclaimers'] = root_element.find('disclaimers').text
        xml_data['notices'] = root_element.find('notices').text
        xml_data['expand'] = root_element.find('expand').text
        xml_data['tidetype'] = root_element.find('tideType').text
        xml_data['tidal'] = root_element.find('tidal').text
        xml_data['greatlakes'] = root_element.find('greatlakes').text
        xml_data['shefcode'] = root_element.find('shefcode').text
        xml_data['details'] = root_element.find('details').text
        xml_data['sensors'] = root_element.find('sensors').text
        xml_data['floodlevels'] = root_element.find('floodlevels').text
        xml_data['datums'] = root_element.find('datums').text
        xml_data['supersededdatums'] = root_element.find('supersededdatums').text
        xml_data['harmonicconstituents'] = root_element.find('harmonicConstituents').text
        xml_data['benchmarks'] = root_element.find('benchmarks').text
        xml_data['tidepredoffsets'] = root_element.find('tidePredOffsets').text
        xml_data['state'] = root_element.find('state').text
        xml_data['timezone'] = root_element.find('timezone').text
        xml_data['timezonecorr'] = root_element.find('timezonecorr').text
        xml_data['observedst'] = root_element.find('observedst').text
        xml_data['stormsurge'] = root_element.find('stormsurge').text
        xml_data['nearby'] = root_element.find('nearby').text
        xml_data['forecast'] = root_element.find('forecast').text
        xml_data['nonnavigational'] = root_element.find('nonNavigational').text
        yield xml_data

##############
from pandas.io.json import json_normalize
def retrieve_noaa_weather_data(stationid='', product='', begin_date='', end_date='', units ='metric'):
    #headers: access_token
    r = requests.get(f'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date={begin_date}&end_date={end_date}&station={stationid}&product={product}&units={units}&time_zone=gmt&application=ports_screen&format=json', headers=my_headers)
    #results = r.json()['results']
    data = r.json()['data']
    #df = json_normalize(results)
    df = json_normalize(data)
    return(df)

# Note: For retrieve_noaa_weather_data(), I am fixing bugs with reading the json for multiple requested datatypes and getting all records for provided begin_date and end_date (for beyond 31 day retrieval) with nested for loops, but the function above works for one datatype for sure (product='air_pressure').
# test = retrieve_noaa_weather_data(stationid='9413450', product='air_pressure', begin_date='20210226 18:42', end_date='20210227 00:00', units ='metric'


# This function takes as an input the dataframe which will be one of 
# CTD, mDOT, Troll, tidbit, or Other_data and adds on the buoy data
# @Zaib if the stations xml is static, then you can maybe make it a dataframe and put it in the database as a table
# otherwise, feel free to stick it in this folder and .gitignore it
# Use this function as the main function. It will be called in logger.py so that the columns can be added to the dataframe that they gave data for
def yeahbuoy(df):

    df = df.assign(
        pressure_cmh2o = -88,
        baropressure_mbar = -88,
        baropressure_cmh2o = -88,
        pressure_corr = -88,
        wc_m = -88
    )

    return df