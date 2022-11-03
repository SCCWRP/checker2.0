# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic

def sav(all_dfs):
    
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

    print("Begin SAV Logic Checks...")
    # Logic Check 1: sav_metadata & savpercentcover_data
    # Logic Check 1a: savmeta records not found in savper
    args.update({
        "dataframe": savmeta,
        "tablename": "tbl_sav_metadata",
        "badrows": checkLogic(savmeta, savper, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'savbedreplicate', 'transectreplicate', 'projectid'], df1_name = "SAV_metadata", df2_name = "SAVpercentcover_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, savbedreplicate, transectreplicate,projectid",
        "error_type": "Logic Error",
        "error_message": "Records in SAV_metadata must have corresponding records in SAVpercentcover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sav_metadata records not found in savpercent_data") 
    # Logic Check 1b: savmeta records missing for records provided by savper
    args.update({
        "dataframe": savper,
        "tablename": "tbl_savpercentcover_data",
        "badrows": checkLogic(savper, savmeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'savbedreplicate', 'transectreplicate', 'projectid'], df1_name = "SAVpercentcover_data", df2_name = "SAV_metadata"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, savbedreplicate, transectreplicate,projectid",
        "error_type": "Logic Error",
        "error_message": "Records in SAVpercentcover_data must have corresponding records in SAV_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sav_metadata records missing for records provided in  savpercent_data") 

    print("End SAV Logic Checks...")
    
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
        "badrows":savmeta[(savmeta['transectlength_m'] > 50) & (savmeta['transectlength_m'] != -88)].index.tolist(),
        "badcolumn": "transectlength_m",
        "error_type" : "Value out of range",
        "error_message" : "Your transect length exceeds 50 m. A value over 50 will be accepted, but is not expected."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran - tbl_sav_metadata - transectlength range")

    #(3) mulitcolumn check for species (scientificname, commonname, status) for tbl_savpercentcover_data

    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"
        
        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        df_to_check['status'] = df_to_check['status'].astype(str)
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())
        
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)


    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(savper, lu_species, check_cols, lookup_cols)
    
    args.update({
        "dataframe": savper,
        "tablename": "tbl_savpercentcover_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check ran - savpercentcover_data - multicol species")
    
    return {'errors': errs, 'warnings': warnings}