# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
import datetime as dt
import pandas as pd
from datetime import date
from .functions import checkData, get_badrows

def feldspar(all_dfs):
    
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

    felddata = all_dfs['tbl_feldspar_data']
    feldmeta = all_dfs['tbl_feldspar_metadata']

    errs = []
    warnings = []

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

    args.update({
        "dataframe": felddata,
        "tablename": "tbl_feldspar_data",
        "badrows":felddata['samplecollectiondate'] == felddata['samplecollectiondate'].dt.date.index.tolist(),
        "badcolumn": "samplecollectiondate",
        "error_type" : "Date Value out of range",
        "error_message" : "Your Date format is not correct, must be YYYY-MM-DD."
    })
    errs = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": feldmeta,
        "tablename": "tbl_feldspar_metadata",
        "badrows":feldmeta['samplecollectiondate'] == feldmeta['samplecollectiondate'].dt.date.index.tolist(),
        "badcolumn": "samplecollectiondate",
        "error_type" : "Date Value out of range",
        "error_message" : "Your Date format is not correct, must be YYYY-MM-DD."
    })
    errs = [*warnings, checkData(**args)]
    
    return {'errors': errs, 'warnings': warnings}