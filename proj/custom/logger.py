# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows
from .yeahbuoy import yeahbuoy

def logger(all_dfs):
    
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

    loggermeta =  all_dfs['tbl_wq_logger_metadata']
    loggerm =     all_dfs['tbl_logger_mdot_data']
    loggerc =     all_dfs['tbl_logger_ctd_data']
    loggertroll = all_dfs['tbl_logger_troll_data']
    loggertid =   all_dfs['tbl_logger_tidbit_data']
    loggerother = all_dfs['tbl_logger_other_data']

   
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
    
    # Before the Barometric/Buoy routine
    nonrequired = {
        'tbl_logger_mdot_data': loggerm,
        'tbl_logger_ctd_data': loggerc,
        'tbl_logger_troll_data': loggertroll,
        'tbl_logger_tidbit_data': loggertid,
        'tbl_logger_other_data': loggerother
    }

    # first check if they have all required data
    if all([df.empty for df in nonrequired.values()]):
        for k, df in nonrequired.items():
            # add a row to the empty dataframe
            df = pd.DataFrame(eval( "{}".format({col: '' for col in df.columns}) ) , index = [0])
            
            args.update({
                "dataframe": df,
                "tablename": k,
                "badrows":[0],
                "badcolumn": ",".join([col for col in df.columns]),
                "error_type" : "Missing required data",
                "error_message" : f"Data must be provided for one of {', '.join(tbl for tbl in nonrequired.keys())}"
            })
            errs = [*errs, checkData(**args)]

            # return here if they dont have all the required data
            return {'errors': errs, 'warnings': warnings}

    else:
        for df in nonrequired:
            df = yeahbuoy(df) if not df.empty else df

    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['qvalue'] < 0.9) & (loggerm['qvalue']!=-88)].index.tolist(),
        "badcolumn": "qvalue",
        "error_type" : "Value out of range",
        "error_message" : "Your qvalue is out of range. Must be greater than 0."
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['do_percent'] < 0) & (loggerm['do_percent']!=-88)].index.tolist(),
        "badcolumn": "do_percent",
        "error_type" : "Value out of range",
        "error_message" : "Your do_percent is out of range, must be greater than 0."
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "dataframe": loggerm,
        "tablename": "tbl_logger_mdot_data",
        "badrows":loggerm[(loggerm['do_mgl'] < 0) & (loggerm['do_mgl']!=-88)].index.tolist(),
        "badcolumn": "do_mgl",
        "error_type" : "Date Value out of range",
        "error_message" : "Your do_mql value is out of range. Must be greater than 0."
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "dataframe": loggerc,
        "tablename": "tbl_logger_ctd_data",
        "badrows":loggerc[(loggerc['conductivity_sm'] < 0) & (loggerc['conductivity_sm'] != -88)].index.tolist(),
        "badcolumn": "conductivity_sm",
        "error_type" : "Value out of range",
        "error_message" : "Your conductivity_sm value is out of range, it must be equal or greater than 0."
    })
    warnings = [*warnings, checkData(**args)]


    
    return {'errors': errs, 'warnings': warnings}