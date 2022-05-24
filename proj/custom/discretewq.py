# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic

def discretewq(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
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

    
    watermeta=all_dfs['tbl_waterquality_metadata']
    waterdata=all_dfs['tbl_waterquality_data']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename":'',
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

    print("Begin WQ Logic Checks...")
    # Logic Check 1: wq_metadata & wq_data
    # Logic Check 1a: wq_metdata records not found in wq_data
    args.update({
        "dataframe": watermeta,
        "tablename": "tbl_waterquality_metadata",
        "badrows": checkLogic(watermeta, waterdata, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplecollectiontime', 'profile', 'depth_m'], df1_name = "WQ_metadata", df2_name = "WQ_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m",
        "error_type": "Logic Error",
        "error_message": "Each record in WQ_metadata must have a corresponding record in WQ_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - wq_metadata records not found in wq_data") #testing

    # Logic Check 1b: wq_metadata records missing for records provided by wq_data
    args.update({
        "dataframe": waterdata,
        "tablename": "tbl_waterquality_data",
        "badrows": checkLogic(waterdata, watermeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplecollectiontime', 'profile', 'depth_m'], df1_name = "WQ_data", df2_name = "WQ_metadata"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplecollectiontime, profile, depth_m",
        "error_type": "Logic Error",
        "error_message": "Records in WQ_data must have a corresponding record in WQ_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - wq_metadata records missing for records provided by wq_data") #testing
    print("End WQ Logic Checks...")

    # End Discrete WQ Logic Checks
    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows":watermeta[(watermeta['depth_m'] < 0)].index.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Your depth value must be larger than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - negative depth - wq_metadata")
    
    #commenting this out because time check in core checks should take care of it - testing this out
    '''
    args.update({
        "dataframe": watermeta,
        "tablename": 'tbl_waterquality_metadata',
        "badrows":watermeta['samplecollectiontime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M:%S') if not (pd.isnull(x) or str(x).lower() == 'not recorded') else "00:00:00").index.tolist(),
        "badcolumn": "samplecollectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your time input is out of range."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - time error - wq_metadata")
    '''

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['conductivity_mscm'] < 0) | (waterdata['conductivity_mscm'] > 100)) & (waterdata['conductivity_mscm'] != -88)].index.tolist(), 
        "badcolumn": "conductivity_mscm",
        "error_type": "Value out of range",
        "error_message" : "Your conductivity value is out of range. Conductivtiy must be between 0 and 100."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['tds_ppt'] < 0) | (waterdata['tds_ppt'] > 100)) & (waterdata['tds_ppt'] != -88)].index.tolist(),
        "badcolumn": "tds_ppt",
        "error_type": "Value Out of range",
        "error_message" : "Your ppt is out of range [0 to 100]"
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['ph_teststrip'] < 1) | (waterdata['ph_teststrip'] > 14)) & (waterdata['ph_teststrip'] != -88)].index.tolist(), 
        "badcolumn": "ph_teststrip",
        "error_type": "Value Out of range",
        "error_message" : "Your ph_teststrip value is out of range."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['ph_probe'] < 1) | (waterdata['ph_probe'] > 14)) & (waterdata['ph_probe'] != -88)].index.tolist(),
        "badcolumn": "ph_probe",
        "error_type": "Value Out of range",
        "error_message" : "Your pH probe value is out of range."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['salinity_ppt'] < 0) | (waterdata['salinity_ppt'] > 100)) & (waterdata['salinity_ppt'] != -88)].index.tolist(),
        "badcolumn": "salinity_ppt",
        "error_type": "Value Out of range",
        "error_message" : "Your sality ppt value is out of range."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['do_mgl'] < 0) | (waterdata['do_mgl'] > 20)) & (waterdata['do_mgl'] != -88)].index.tolist(),
        "badcolumn": "do_mgl",
        "error_type": "Value Out of range",
        "error_message" : "Your DO mgL value is out of range."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['airtemp_c'] < 0) | (waterdata['airtemp_c'] > 50)) & (waterdata['airtemp_c'] != -88)].index.tolist(),
        "badcolumn": "airtemp_c",
        "error_type": "Value Out of range",
        "error_message" : "Your air temp value is out of range."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": waterdata,
        "tablename": 'tbl_waterquality_data',
        "badrows": waterdata[((waterdata['h2otemp_c'] < 0) | (waterdata['h2otemp_c'] > 50)) & (waterdata['h2otemp_c'] != -88)].index.tolist(),
        "badcolumn": "h2otemp_c",
        "error_type": "Value Out of range",
        "error_message" : "Your H2o temp value is out of range."
    })
    errs = [*errs, checkData(**args)]

    return {'errors': errs, 'warnings': warnings}