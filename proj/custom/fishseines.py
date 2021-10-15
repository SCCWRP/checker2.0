# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
import pandas as pd
from .functions import checkData, get_badrows

def fishseines(all_dfs):
    
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
    
    fishabud = all_dfs['tbl_fish_abundance_data']
    fishdata = all_dfs['tbl_fish_length_data']
    fishmeta = all_dfs['tbl_fish_sample_metadata']

   
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
        "dataframe": fishabud,
        "tablename": "tbl_fish_abudance_data",
        "badrows":fishabud[(fishabud['abundance'] < 0) | (fishabud['abundance'] > 1000)].index.tolist(),
        "badcolumn": "abundance",
        "error_type" : "Value out of range",
        "error_message" : "Your abundance value must be between 0 to 1000."
    })
    errs = [*warnings, checkData(**args)]

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta['starttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%I:%M %p') if not pd.isnull(x) else "00:00:00").index.tolist(),
        "badcolumn": "starttime",
        "error_type" : "Start time is not in the correct format.",
        "error_message" : "Start time format should be 12 HR AM/PM."
    })
    errs = [*warnings, checkData(**args)]

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta['endtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%I:%M %p') if not pd.isnull(x) else "00:00:00").index.tolist(),
        "badcolumn": "endtime",
        "error_type" : "End time is not in the correct format",
        "error_message" : "End time format should be 12 HR AM/PM."
    })
    errs = [*warnings, checkData(**args)]

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[fishmeta['starttime'].apply(pd.Timestamp) > fishmeta['endtime'].apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "starttime, endtime",
        "error_type" : "Start time value is out of range.",
        "error_message" : "Start time should be before end time"
    })
    errs = [*warnings, checkData(**args)] 

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[(fishmeta['netbeginlongitude'] < -114.0430560959) | (fishmeta['netendlongitude'] > -124.5020404709)].index.tolist(),
        "badcolumn": "netbeginlatitude,netbeginlongitude",
        "error_type" : "Longitude is out of range",
        "error_message" : "Your longitude coordinates are outside of california, check your minus sign in your longitude data."
    })
    errs = [*warnings, checkData(**args)] 

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[(fishmeta['netbeginlatitude'] < 32.5008497379) | (fishmeta['netendlatitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "netendlatitude,netendlongitude",
        "error_type" : "Latitude is out of range",
        "error_message" : "Your latitude coordinates are outside of california."
    })
    errs = [*warnings, checkData(**args)] 



    
    return {'errors': errs, 'warnings': warnings}