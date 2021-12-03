# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows

def benthicinfauna_field(all_dfs):
    
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

    benthicmeta = all_dfs['tbl_benthicinfauna_metadata']
    #benthiclabbatch = all_dfs['tbl_benthicinfauna_labbatch']
    #benthicabundance = all_dfs['tbl_benthicinfauna_abundance']
    #benthicbiomass = all_dfs['tbl_benthicinfauna_biomass']
    

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    '''
    args = {
        "dataframe": df,
        "tablename": tbl,
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    '''
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


    
    return {'errors': errs, 'warnings': warnings}

def benthicinfauna_lab(all_dfs):
    
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

    #benthicmeta = all_dfs['tbl_benthicinfauna_metadata']
    benthiclabbatch = all_dfs['tbl_benthicinfauna_labbatch']
    benthicabundance = all_dfs['tbl_benthicinfauna_abundance']
    benthicbiomass = all_dfs['tbl_benthicinfauna_biomass']
    

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    '''
    args = {
        "dataframe": df,
        "tablename": tbl,
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    '''
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": '',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_biomass",
        "badrows":benthicbiomass[(benthicbiomass['biomass_gm'] < 0)].index.tolist(),
        "badcolumn": "biomass_gm",
        "error_type" : "Value is out of range.",
        "error_message" : "Biomass must be greater than 0"
    })
    errs = [*errs, checkData(**args)]
    
    args.update({
        "dataframe": benthiclabbatch,
        "tablename": "tbl_benthicinfauna_labbatch",
        "badrows":benthiclabbatch['preparationtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%HH:%MM') if not pd.isnull(x) else "00:00:00").index.tolist(),
        "badcolumn": "preparationtime",
        "error_type" : "Value is out of range.",
        "error_message" : "Time format is not correct."
    })
    errs = [*errs, checkData(**args)]  

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    '''
    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        #bug fix: read 'status' as string to avoid merging on float64 (from df_to_check) and object (from lookup_df) error
        df_to_check['status'] = df_to_check['status'].astype(str)
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname', 'status']

    badrows = multicol_lookup_check(benthicbiomass, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": benthicbiomass,
        "tablename": "tbl_benthicinfauna_abundance",
        "badrows": badrows,
        "badcolumn":"scientificname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname/status entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check ran - benthicinfauna_abundance - multicol species") 

    badrows = multicol_lookup_check(benthiclabbatch, lu_species, check_cols, lookup_cols)

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
    print("check ran - fish_length_data - multicol species") 

    '''
    return {'errors': errs, 'warnings': warnings}