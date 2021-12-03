# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows

def macroalgae(all_dfs):
    
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

    algaemeta = all_dfs['tbl_macroalgae_sample_metadata']
    algaecover = all_dfs['tbl_algaecover_data']
    algaefloating = all_dfs['tbl_floating_data']

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


    
   # return {'errors': errs, 'warnings': warnings}


    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        df_to_check['status'] = df_to_check['status'].astype(str)
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname', 'status']

    badrows = multicol_lookup_check(algaecover, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": badrows,
        "badcolumn":"scientificname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname/status entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check ran - algeacover_data - multicol species") 

    badrows = multicol_lookup_check(algaefloating, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": badrows,
        "badcolumn": "scientificname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname/status entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species

    })

    errs = [*errs, checkData(**args)]
    print("check ran - floating_data - multicol species") 

    
    return {'errors': errs, 'warnings': warnings}


    #TransectBeginLatitude, TransectEndLatitude
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows":algaemeta[(algaemeta['transectbeginlatitude'] < 32.5008497379)].index.tolist(),
        "badcolumn": "transectbeginlatitude",
        "error_type" : "Value out of range",
        "error_message" : "Your latitude coordinate is outside of california."
    })
    errs = [*warnings, checkData(**args)]

    
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows":algaemeta[(algaemeta['transectendlatitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "transectendlatitude",
        "error_type" : "Value out of range",
        "error_message" : "Your latitude coordinate is outside of california."
    })
    errs = [*warnings, checkData(**args)]





    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows":algaemeta[(algaemeta['transectbeginlongitude'] < 32.5008497379)].index.tolist(),
        "badcolumn": "transectbeginlongitude",
        "error_type" : "Value out of range",
        "error_message" : "Your longitude coordinate is  outside of california."
    })
    errs = [*warnings, checkData(**args)]

    
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows":algaemeta[(algaemeta['transectendlongitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "transectendlongitude",
        "error_type" : "Value out of range",
        "error_message" : "Your longitude coordinate is outside of california."
    })
    errs = [*warnings, checkData(**args)]