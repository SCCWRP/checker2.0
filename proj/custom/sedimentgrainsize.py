# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from pandas import DataFrame
from .functions import checkData, get_badrows, checkLogic

def sedimentgrainsize_lab(all_dfs):
    
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
    
    sed = all_dfs['tbl_sedgrainsize_data']
    sedbatch = all_dfs['tbl_sedgrainsize_labbatch_data']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": sed,
        "tablename": 'tbl_sedgrainsize_data',
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

    # Logic Checks
    eng = g.eng
    sql = eng.execute("SELECT * FROM tbl_sedgrainsize_metadata")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    sedmeta = sql_df
    del sql_df
    print("Begin Sediment Grain Size Lab Logic Checks...")
    # Logic Check 1: sedimentgrainsize_metadata (db) & sediment_labbatch_data (submission), sedimentgrainsize_metadata records do not exist in database
    args = {
        "dataframe": sedbatch,
        "tablename": 'tbl_sedgrainsize_labbatch_data',
        "badrows": checkLogic(sedbatch, sedmeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'matrix', 'samplelocation'], df1_name = "SedimentGrainSize_labbatch_data", df2_name = "SedimentGrainSize_metadata"),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, matrix, samplelocation",
        "error_type": "Logic Error",
        "error_message": "Field submission for sediment grain size labbatch data is missing. Please verify that the sediment grain size field data has been previously submitted."
    }
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sediment grain size metadata records do not exist in database for sediment grain size labbatch data submission")

    # Logic Check 2: sedgrainsize_labbatch_data & sedgrainsize_data must have corresponding records within session submission
    # Logic Check 2a: sedgrainsize_data missing records provided by sedgrainsize_labbatch_data
    args.update({
        "dataframe": sedbatch,
        "tablename": "tbl_sedgrainsize_labbatch_data",
        "badrows": checkLogic(sedbatch, sed, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'preparationbatchid'], df1_name = "SedimentGrainSize_labbatch_data", df2_name = "SedGrainSize_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, preparationbatchid",
        "error_type": "Logic Error",
        "error_message": "Records in sedimentgrainsize_labbatch_data must have corresponding records in sedgrainsize_data. Missing records in sedgrainsize_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing sedgrainsize_data records")

    
    return {'errors': errs, 'warnings': warnings}

def sedimentgrainsize_field(all_dfs):
    
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
    
    meta = all_dfs['tbl_sedgrainsize_metadata']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": meta,
        "tablename": 'tbl_sedgrainsize_metadata',
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


    
    return {'errors': errs, 'warnings': warnings}