# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
import pandas as pd
from .functions import checkData, get_badrows

def vegetation(all_dfs):
    
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
    
    vegmeta = all_dfs['tbl_vegetation_sample_metadata']
    vegdata = all_dfs['tbl_vegetative_coverdata']
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe":pd.DataFrame({}),
        "tablename":'',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetative_coverdata",
        "badrows":vegdata[(vegdata['tallestplantheight_cm']<0) | (vegdata['tallestplantheight_cm'] > 300)].index.tolist(),
        "badcolumn": "tallestplantheight_cm",
        "error_type" : "Value is out of range.",
        "error_message" : "Height should be between 0 to 3 metres"
    })
    errs = [*warnings, checkData(**args)]

    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < -114.0430560959) | (vegmeta['transectendlongitude'] > -124.5020404709)].index.tolist(),
        "badcolumn": "transectbeginlongitude,transectendlongitude",
        "error_type" : "Value out of range",
        "error_message" : "Your longitude coordinates are outside of california, check your minus sign in your longitude data."
    })
    errs = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < 32.5008497379) | (vegmeta['transectendlongitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "transectbeginlatitude,transectendlatitude",
        "error_type" : "Value out of range",
        "error_message" : "Your latitude coordinates are outside of california."
    })
    errs = [*warnings, checkData(**args)]

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]


    
    return {'errors': errs, 'warnings': warnings}