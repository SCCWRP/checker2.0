from copy import deepcopy
from flask import current_app
from inspect import currentframe
from .functions import checkData, get_badrows
import pandas as pd

def monitoring(all_dfs):
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
    
    # This data type should have tbl_ceden_waterquality, tbl_precipitation, and flow
    wq = all_dfs['tbl_ceden_waterquality']
    precip = all_dfs['tbl_precipitation']
    flow = all_dfs['tbl_flow']

    errs = []
    warnings = []
    
    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": 'tbl_test',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    wq_args = deepcopy(args)
    wq_args.update({"dataframe": wq, "tablename": 'tbl_ceden_waterquality'})

    flow_args = deepcopy(args)
    flow_args.update({"dataframe": flow, "tablename": 'tbl_flow'})

    precip_args = deepcopy(args)
    precip_args.update({"dataframe": precip, "tablename": 'tbl_precipitation'})

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]



    # --- Water Quality checks --- #

    # --- End Water Quality checks --- #



    # --- Flow checks --- #

    # --- End Flow checks --- #



    # --- Precipitation checks --- #

    # --- End Precipitation checks --- #


    
    return {'errors': errs, 'warnings': warnings}