# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from pandas import DataFrame
import pandas as pd
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
        "badrows": checkLogic(sedbatch, sed, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'preparationbatchid', 'labreplicate'], df1_name = "SedimentGrainSize_labbatch_data", df2_name = "SedGrainSize_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, preparationbatchid, labreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sedimentgrainsize_labbatch_data must have corresponding records in sedgrainsize_data. Missing records in sedgrainsize_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing sedgrainsize_data records")

    # Logic Check 2b: sedgrainsize_labbatch_data missing records provided by sedgrainsize_data
    tmp = sed.merge(
        sedbatch.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'samplelocation', 'preparationbatchid', 'matrix', 'labreplicate'],
        how = 'left'
    )
    badrows = tmp[pd.isnull(tmp.present)].index.tolist()
    args.update({
        "dataframe": sed,
        "tablename": "tbl_sedgrainsize_data",
        "badrows": badrows,
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, samplelocation, preparationbatchid, matrix, labreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sedgrainsize_data must have corresponding records in sedgrainsize_labbatch_data. Missing records in sedgrainsize_labbatch_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - missing sedgrainsize_labbatch_data records")

    print("End Sediment Grain Size Lab Logic Checks...")

    
    return {'errors': errs, 'warnings': warnings}

def sedimentgrainsize_field(all_dfs):
    
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

    # Multi column Lookup Check 
    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        if 'status' in df_to_check.columns.tolist():
            df_to_check['status'] = df_to_check['status'].astype(str)
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    print("Begin SedGrainSize Multicol Checks to check SiteID/EstuaryName pair...")
    lookup_sql = f"SELECT * FROM lu_siteid"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']

    # Multicol - sedgrainsize_meta
    args.update({
        "dataframe": meta,
        "tablename": "tbl_sedgrainsize_metadata",
        "badrows": multicol_lookup_check(meta, lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - sedgrainsize_metadata")
    errs = [*errs, checkData(**args)]
    print("End eDNA Multicol Checks to check SiteID/EstuaryName pair.")
    
    return {'errors': errs, 'warnings': warnings}