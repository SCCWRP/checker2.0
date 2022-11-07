

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

    # read in samplecollectiondate as pandas datetime so merging bruv dfs (incl metadata) without dtype conflict
    bruvvideo['samplecollectiondate'] = pd.to_datetime(bruvvideo['samplecollectiondate'])
    bruvdata['samplecollectiondate'] = pd.to_datetime(bruvdata['samplecollectiondate'])

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

    ##################################### CUSTOM CHECKS ######################################################
    # Custom Checks
    # Check 1: bruv_videolog MaxNs col must be a postive integer 
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[bruvdata['maxns'] < 0].index.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "MaxNs must be a positive integer or left blank."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - bruv metadata records do not exist in database for bruv lab submission") #tested

    # Check: if in_out = 'out', maxn & maxns MUST be NULL
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (~bruvdata['scientificname'].isin(['unknown fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'out') & 
            ((~bruvdata['maxn'].isna()) | (~bruvdata['maxns'].isna()))].index.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'out', then both MaxN/MaxNs must be empty."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - value - bruv_data - invalid maxn/maxns value (out)") #tested #working

    # Check: if in_out = 'in', maxn & maxns MUST be INTEGER values
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[
            (~bruvdata['scientificname'].isin(['unknown fish','unknown juvenile fish','unknown crab'])) &
            (bruvdata['in_out'] == 'in') & 
            ((bruvdata['maxn'].isna()) | (bruvdata['maxns'].isna()))
            ].index.tolist(),
        "badcolumn": "in_out, maxn, maxns",
        "error_type": "Value Error",
        "error_message": "Invalid entry for MaxN/MaxNs column. Since in_out column is 'in', then both MaxN/MaxNs must have an integer value and cannot be left empty."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - value - bruv_data - invalid maxn/maxns value (in)") #tested #working

    # Check: MaxN = sum(MaxNs) for i = 0, 1,2,.., where i is the rows per grouped_df on grouped_cols
    # grouped_cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','foventeredtime','fovlefttime','in_out'] 
    cols = ['siteid','estuaryname','stationno','samplecollectiondate','camerareplicate','videoorder','foventeredtime','fovlefttime','in_out'] 
    # keep origial indices for marking file
    bruvdata['tmp_row'] = bruvdata.index
    #subsetting for 'in_out' == 'in' so that there are fewer keys to loop through
    grouped_df = bruvdata[bruvdata['in_out'] == 'in'].groupby(cols) 
    gb = grouped_df.groups
    key_list_from_gb = gb.keys()
    badrows = [] #initialized to append
    for key, values in gb.items():
        if key in key_list_from_gb: #this is by unique group
            tmp = bruvdata.loc[values]
            #print("=============== printing tmp for maxn maxns check ==================")
            #print(tmp)
            #print("=============== tmp printed ==================")
            brows = tmp[(tmp['maxn'] != tmp['maxns'].sum())].tmp_row.tolist()
            #extend adds second list elts to first list
            badrows.extend(brows) #this will be populated to the badrows key in the args dict
    bruvdata = bruvdata.drop(columns=['tmp_row'])
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": badrows,
        "badcolumn": "maxns, maxn",
        "error_type": "Value Error",
        "error_message": "MaxN is the sum of all/each species counts (MaxNs) within a given frame. Each record with the same FOV frame should have the same value for MaxN."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - value - bruv_data - maxn as sum of maxns by group")
    # Check 2: bruv_data check on ['in_out','certainty']
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata[(bruvdata['in_out'] == 'in') & (pd.isnull(bruvdata['certainty']))].index.tolist(),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Value Error",
        "error_message": "Invalid entry for certainty column. Since in_out column is 'in', then certainty must have a nonempty value."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - value - bruv_data - invalid certainty value") #tested

    # Check: if scientificname = unknown fish, then MaxNs must be empty
    args = {
        "dataframe": bruvdata,
        "tablename": 'tbl_bruv_data',
        "badrows": bruvdata.query("scientificname == 'unknown fish' and not maxns.isnull()").index.tolist(),
        "badcolumn": "scientificname, maxns",
        "error_type": "Value Error",
        "error_message": "If scientificname = unknown fish, then MaxNs must be empty"
    }
    errs = [*errs, checkData(**args)]
    print("check ran - if scientificname = unknown fish, then MaxNs must be empty")
    ##################################### FINISH CUSTOM CHECKS ######################################################
    
    
    
    
    ##################################### LOGIC CHECKS ######################################################
    print("Begin BRUV Lab Logic Checks...")
    eng = g.eng
    bruvmeta = pd.read_sql("SELECT * FROM tbl_bruv_metadata", eng)
    
    ## NOTE: Logic Checks (Against Database)
    # Each videolog data must correspond to bruv_metadata in database
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index),
        bruvmeta, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()

    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate",
        "error_type": "Logic Error",
        "error_message": "Records in bruv_videolog should have corresponding records in bruv_metadata. Please submit the metadata for these records first."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - Each videolog data must correspond to metadata in database")

    ## NOTE: Logic Checks (within Submission)
    # Logic Check 1: Each metadata (videolog) with fish is yes and bait is visible should have a corresponding record in data
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index).query("fish == 'Yes' and bait == 'visible'"),
        bruvdata, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()

    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "We have metadata in videolog where fish is yes, and bait is visible, but no corresponding data in bruv_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - Each metadata (videolog) with fish is yes and bait is visible should have a corresponding record in data")
    
    # Logic Check 2: Each metadata (videolog) with fish is no should not have a corresponding record in data
    badrows = pd.merge(
        bruvvideo.assign(tmp_row=bruvvideo.index).query("fish == 'No'"),
        bruvdata, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'both'").get('tmp_row').tolist()
    
    args.update({
        "dataframe": bruvvideo,
        "tablename": "tbl_bruv_videolog",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "Each metadata (videolog) with fish is no should not have a corresponding record in data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran -  If fish is 'no' in videolog then data should not have corresponding records")

    # Logic Check 3: Each data (bruv_data) should have a corresponding record in videolog
    badrows = pd.merge(
        bruvdata.assign(tmp_row=bruvdata.index),
        bruvvideo, 
        on=['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'camerareplicate', 'filename', 'videoorder', 'projectid'],
        how='left',
        indicator='in_which_df'
    ).query("in_which_df == 'left_only'").get('tmp_row').tolist()
    
    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, camerareplicate, filename, videoorder, projectid",
        "error_type": "Logic Error",
        "error_message": "Each data (bruv_data) should have a corresponding record in videolog."
    })
    errs = [*errs, checkData(**args)]
    print("check ran -  Each data (bruv_data) should have a corresponding record in videolog")        

    ##################################### FINISH LOGIC CHECKS ######################################################
    
    
    # Multicol lookup check
    def multicol_lookup_check(df_to_check,lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")

        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())
        
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    print("read in fish lookup")
    lookup_sql = f"SELECT * from lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    #check_cols = ['scientificname', 'commonname']
    lookup_cols = ['scientificname', 'commonname', 'status']
    #lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(bruvdata, lu_species, check_cols, lookup_cols)
    
    args.update({
        "dataframe":  bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows,
        "badcolumn": "scientificname,commonname,status",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : f'The scientificname/commonname/status entry did not match the lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check ran - bruv_data - multicol species")

    return {'errors': errs, 'warnings': warnings}