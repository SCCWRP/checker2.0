# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows

def sav(all_dfs):
    
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
    
    savmeta = all_dfs['tbl_sav_metadata']
    savper = all_dfs['tbl_savpercentcover_data']
    
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe":pd.DataFrame({}),
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
    
    #(1) transectlength_m is nonnegative # tested
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows":savmeta[(savmeta['transectlength_m'] < 0) & (savmeta['transectlength_m'] != -88)].index.tolist(),
        "badcolumn": "transectlength_m",
        "error_type" : "Value out of range",
        "error_message" : "Your transect length must be nonnegative."
    })
    errs = [*errs, checkData(**args)]

    #(2) transectlength_m range check [0, 50] #tested
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows":savmeta[((savmeta['transectlength_m'] < 0) | (savmeta['transectlength_m'] > 50)) & (savmeta['transectlength_m'] != -88)].index.tolist(),
        "badcolumn": "transectlength_m",
        "error_type" : "Value out of range",
        "error_message" : "Your transect length exceeds 50 m. A value over 50 will be accepted, but is not expected."
    })
    warnings = [*warnings, checkData(**args)]
    
    
    return {'errors': errs, 'warnings': warnings}