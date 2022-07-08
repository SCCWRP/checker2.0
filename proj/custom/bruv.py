

from inspect import currentframe
import pandas as pd
from pandas import DataFrame
from flask import current_app, g
from .functions import checkData, get_badrows, checkLogic
import re
import time

#define new function called 'bruvlab' for lab data dataset - 
#change to bruvmeta, need to make changes to the visual map check
def bruv_field(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    lu_list_script_root = current_app.script_root

    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # These are the dataframes that got submitted for bruv
    print("define tables")
    protocol = all_dfs['tbl_protocol_metadata']
    bruvmeta = all_dfs['tbl_bruv_metadata']
    #bruvdata = all_dfs['tbl_bruv_data'] #leaving tbl_bruv_data out for later, this is lab data and will not be submitted with the metadata tables - Zaib

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # Im just initializing the args dictionary
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    #(1) maxnspecies is nonnegative
    '''
    timeregex ="([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata['maxnspecies'] < 0].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        #"error_message":"Max number of species should be between 0 and 100"
        "error_message":"Max number of species must be nonnegative."
    })
    errs = [*errs, checkData(**args)]

    #(2) maxnspecies should not exceed 100 (warning)
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[(bruvdata['maxnspecies'] < 0) | (bruvdata['maxnspecies'] > 100)].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        "error_message":"Max number of species should NOT exceed 100."
    })
    warnings = [*warnings, checkData(**args)]

    
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata.foventeredtime.apply(pd.Timestamp) > bruvdata.fovlefttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn":"foventeredtime,fovlefttime",
        "error_type": "Value out of range",
        "error_message":"FOV entered time must be before FOV left time"
    })
    errs = [*errs, checkData(**args)]
    '''



    print("bruvmeta.head()['bruvintime']")
    print(bruvmeta.head()['bruvintime'])
    print("bruvmeta.head()['bruvouttime']")
    print(bruvmeta.head()['bruvouttime'])


    # Check: bruvmetadata bruvintime time validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    badrows_bruvintime = bruvmeta[
        bruvmeta['bruvintime'].apply(
            lambda x: not bool(re.match(timeregex, str(x))) if str(x) != "00:00:00" else False)
            ].index.tolist()
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": badrows_bruvintime,
        "badcolumn": "bruvintime",
        "error_type" : "Time Format Error",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - bruv_metadata - bruvintime") 
    # Check: bruvmetadata bruvouttime time validation
    badrows_bruvouttime = bruvmeta[
        bruvmeta['bruvouttime'].apply(
            lambda x: not bool(re.match(timeregex, str(x))) if str(x) != "00:00:00" else False)
            ].index.tolist()
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": badrows_bruvouttime,
        "badcolumn": "bruvouttime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_bruv_metadata - bruvouttime format") 

    # NOTE This check needs to take into consideration that the data is clean if the start date is before the end date
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    '''
    df = bruvmeta[(bruvmeta['bruvintime'] != "00:00:00") & (bruvmeta['bruvouttime'] != "00:00:00")]
    print(" =========================================")
    print("subsetting df on bruv time: ")
    print(" =========================================")
    print(df['bruvintime'])
    print(df['bruvouttime'])
    if (len(badrows_bruvintime) == 0 & (len(badrows_bruvouttime) == 0)):
        args.update({
            "dataframe": bruvmeta,
            "tablename": "tbl_bruv_metadata",
            "badrows": df[df['bruvintime'].apply(
                lambda x: pd.Timestamp(str(x)).strftime('%H:%M') 
                if not "00:00:00" else '') >= df['bruvouttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') 
                if not "00:00:00" else '')].index.tolist(),
            "badcolumn": "bruvintime",
            "error_message": "Bruvintime value must be before bruvouttime."
            })
        errs = [*errs, checkData(**args)]
    print("check ran - tbl_bruv_metadata - bruvintime before bruvouttime")

    del badrows_bruvintime
    del badrows_bruvouttime
    '''
    # Check: depth_m is positive for tbl_bruv_metadata 
    print("before metadata depth check")
    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[(bruvmeta['depth_m'] < 0) & (bruvmeta['depth_m'] != -88)].index.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Depth measurement should not be a negative number, must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_bruv_metadata - nonnegative depth_m") # tested

    return {'errors': errs, 'warnings': warnings}


    

    # Check: bruvintime format validation    24 hour clock HH:MM time validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": bruvmeta[~bruvmeta['bruvintime'].str.match(timeregex)].index.tolist(),
        "badcolumn": "bruvintime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - bruv_metadata - bruvintime format") 

def bruv_lab(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    lu_list_script_root = current_app.script_root

    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    print("did it break here?")
    protocol = all_dfs['tbl_protocol_metadata']
    bruvdata = all_dfs['tbl_bruv_data']
    bruvvideo = all_dfs['tbl_bruv_videolog']
    errs = []
    warnings = []

    print("bruvvideo")
    print(bruvvideo['samplecollectiondate'])
    print("bruvdata")
    print(bruvdata['samplecollectiondate'])

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # Custom Checks
    # Check 1: bruv_videolog Ns col must be a postive integer
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[bruvdata['ns'] < 0].index.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "Ns must be a positive integer. Field cannot be left blank or filled with -88."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - bruv metadata records do not exist in database for bruv lab submission") #tested
    # Logic Checks
    print("Begin BRUV Lab Logic Checks...")
    eng = g.eng
    sql = eng.execute("SELECT * FROM tbl_bruv_metadata")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    bruvmeta = sql_df
    del sql_df
    # Logic Check 1: bruv_metadata (db) & bruv_videolog (submission), bruv_metadata records do not exist in database
    # double check these columns -- seems right for now, but these tables may be revised - zaib 17jun2022
    args = {
        "dataframe": bruvvideo,
        "tablename": 'tbl_bruv_videolog',
        "badrows": checkLogic(bruvvideo, bruvmeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate'], df1_name = "BRUV_videolog", df2_name = "BRUV_metadata"),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Logic Error",
        "error_message": "Field submission for bruv videolog data is missing. Please verify that the bruv field data has been previously submitted."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - bruv metadata records do not exist in database for bruv lab submission") #tested

    # Logic Check 2: bruv_videolog & bruv_data must have corresponding records within session submission iff bruv_videolog['fish'] = yes
    # check  modified by jan's instruction 6jul22
    # Logic Check 2a: bruv_data missing records provided by bruv_videolog (only if fish field is yes)
    tmp = bruvvideo[bruvvideo['fish'] == 'Yes']
    #using tmp instead of bruvvideo for badrows 
    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": checkLogic(tmp, bruvdata, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder'], df1_name = "BRUV_videolog", df2_name = "BRUV_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder",
        "error_type": "Logic Error",
        "error_message": "Records in bruv_videolog have must have corresponding records in bruv_data. Missing records in bruv_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing bruv_data records") #tested

    # Logic Check 2b: bruv_videolog missing records provided by bruv_data
    if bruvdata.empty: #Bug fix: when df empty, date cols are recognized as object. cast type to pandas datetime type.
        bruvdata['samplecollectiondate'] = pd.to_datetime(bruvdata['samplecollectiondate'])
    print("these are the dtypes: ")
    print(bruvdata.dtypes)
    #tmp = bruvvideo[bruvvideo['fish'] == 'Yes']
    tmp = bruvdata.merge(
        bruvvideo.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder'],
        how = 'left'
    )
    badrows = tmp[(pd.isnull(tmp.present)) & (tmp['fish'] == 'Yes')].index.tolist()
    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows,
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder",
        "error_type": "Logic Error",
        "error_message": "Records in bruv_data must have corresponding records in bruv_videolog. Missing records in bruv_videolog."
    })
    errs = [*errs, checkData(**args)]
    del tmp
    print("check ran - logic - missing bruv_videolog records") #tested
    print("End BRUV Lab Logic Checks...")

    # Logic Check 2c: bruv_data should NOT have corresponding records if fish col (in bruvvideo) is 'no'
    #tmp = bruvvideo[bruvvideo['fish'] == 'No']
    #using tmp instead of bruvvideo for badrows 
    tmp = bruvdata.merge(
        bruvvideo.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder'],
        how = 'left'
    )
    badrows = tmp[(tmp['present'] == 'yes') & (tmp['fish'] == 'No')].index.tolist()
    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder",
        "error_type": "Logic Error",
        "error_message": "Records in bruv_videolog should not have corresponding records in bruv_data. If fish column is filled with 'no', then bruv_data should not have a corresponding record."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - extranneous bruv_data records") #testing

    #tbl_bruv_data will have the species column check, yet to be tested
    def multicol_lookup_check(df_to_check,lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        df_to_check['status'] =df_to_check['status'].astype(str)
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)
    print("read in fish lookup")
    lookup_sql = f"SELECT * from lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(bruvdata, lu_species, check_cols, lookup_cols)
    
    args.update({
        "dataframe":  bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows,
        "badcolumn": "scientificname",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : f'The scientificname/commonname/status entry did not match the lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check ran - bruv_data - multicol species")

    return {'errors': errs, 'warnings': warnings}