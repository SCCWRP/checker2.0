

from inspect import currentframe
import pandas as pd
from flask import current_app
from .functions import checkData, get_badrows

def bruv(all_dfs):
    
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
    
    # These are the dataframes that got submitted for bruv
    #protocol = all_dfs['tbl_protocol_metadata']
    bruvmeta = all_dfs['tbl_bruv_metadata']
    bruvdata = all_dfs['tbl_bruv_data']

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
    
    args.update({
        "dataframe":bruvdata,
        "tablename":"tbl_bruv_data",
        "badrows":bruvdata[(bruvdata['maxnspecies'] < 0) | (bruvdata['maxnspecies'] > 100)].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        "error_message":"Max number of species should be between 0 and 100"
    })
    errs = [*errs, checkData(**args)]

    print("errs: ")
    print(errs)
    
    args.update({
        "dataframe":bruvdata,
        "tablename":"tbl_bruv_data",
        "badrows":bruvdata[bruvdata.foventeredtime.apply(pd.Timestamp) > bruvdata.fovlefttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn":"foventeredtime,fovlefttime",
        "error_type": "Value out of range",
        "error_message":"FOV entered time must be before FOV left time"
    })
    errs = [*errs, checkData(**args)]
    
    
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": bruvmeta[bruvmeta.bruvintime.apply(pd.Timestamp) > bruvmeta.bruvouttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "bruvintime,bruvouttime",
        "error_type" : "Time format out of range",
        "error_message" : "Bruvintime must be before bruvouttime"
    })
    errs = [*errs, checkData(**args)]
    
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": bruvmeta[(bruvmeta['depth_m'] < 0)].index.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Depth measurement should not be a negative number, must be greater than 0"
    })
    errs = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": bruvmeta[(bruvmeta['longitude'] < -114.0430560959) | (bruvmeta['longitude'] > -124.5020404709)].index.tolist(),
        "badcolumn": "longitude",
        "error_type" : "Value out of range",
        "error_message" : "Your coordinates incidate you are out of California. Check minus signs for your longitude range"
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": bruvmeta,
        "tablename": "tbl_bruv_metadata",
        "badrows": bruvmeta[(bruvmeta['latitude'] < 32.5008497379) | (bruvmeta['latitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "latitude",
        "error_type" : "Value out of range",
        "error_message" : "Your coordinates incidate you are out of California. Check your latitude range"
    })
    errs = [*errs, checkData(**args)]

    print("what does errs look like? ")
    print(errs)
    
    return {'errors': errs, 'warnings': warnings}