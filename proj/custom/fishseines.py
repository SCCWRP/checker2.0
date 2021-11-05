# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows

def fishseines(all_dfs):
    
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
    print("check ran - tbl_fish_abundance_data - abundance range")
    # commenting out time checks for now - zaib 28 oct 2021
    '''
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": fishmeta[fishmeta['starttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%I:%M %p') if not pd.isnull(x) else "00:00:00")].index.tolist(),
        "badcolumn": "starttime",
        "error_type" : "Start time is not in the correct format.",
        "error_message" : "Start time format should be 12 HR AM/PM."
    })
    errs = [*warnings, checkData(**args)]

    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": fishmeta[fishmeta['endtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%I:%M %p') if not pd.isnull(x) else "00:00:00")].index.tolist(),
        "badcolumn": "endtime",
        "error_type" : "End time is not in the correct format",
        "error_message" : "End time format should be 12 HR AM/PM."
    })
    errs = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows":fishmeta[fishmeta['starttime'].apply(pd.Timestamp) > fishmeta['endtime'].apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "starttime",
        "error_type" : "Start time value is out of range.",
        "error_message" : "Start time should be before end time"
    })
    errs = [*warnings, checkData(**args)] 
    print("check ran - fish_sample_metadata - start time before end time")
    '''

    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        df_to_check['status'] = df_to_check['status'].astype(str)
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        print("merged")
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname', 'status']

    badrows = multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": badrows,
        "badcolumn":"scientificname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname/status entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check ran - fish_abundance_metadata - multicol species") 

    badrows = multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": badrows,
        "badcolumn": "scientificname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname/status entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })

    errs = [*errs, checkData(**args)]
    print("check ran - fish_length_metadata - multicol species") 

    
    return {'errors': errs, 'warnings': warnings}