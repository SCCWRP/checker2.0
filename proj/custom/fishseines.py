# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic
import re
import time

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
    
    print("Begin Fish Seines Logic Checks...")
    # Logic Check 1: fish_sample_metadata & fish_abundance_data
    # Logic Check 1a: fishmeta records not found in fishabud
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": checkLogic(fishmeta, fishabud, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'surveytype', 'netreplicate'], df1_name = "sample_metadata", df2_name = "abundance_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in abundance_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records not found in abundance_data") 
    # Logic Check 1b: fishmeta records missing for records provided by fishabud
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": checkLogic(fishabud, fishmeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'surveytype', 'netreplicate'], df1_name = "abundance_data", df2_name = "sample_metadata"),
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, surveytype, netreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in abundance_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records missing for records provided in abundance_data") 

    print("End Fish Seines Logic Checks...")

    # Check - abundance range [0, 5000]
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows":fishabud[((fishabud['abundance'] < 0) | (fishabud['abundance'] > 5000)) & (fishabud['abundance'] != -88)].index.tolist(),
        "badcolumn": "abundance",
        "error_type" : "Value out of range",
        "error_message" : "Your abundance value must be between 0 to 5000. If this value is supposed to be empty, please fill with -88."
    })
    warnings = [*warnings, checkData(**args)]
    print("check ran - tbl_fish_abundance_data - abundance range") # tested and working 5nov2021
    # commenting out time checks for now - zaib 28 oct 2021
    ## for time fields, in preprocess.py consider filling empty time related fields with NaT using pandas | check format of time?? | should be string

    # Check: starttime format validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    badrows_starttime = fishmeta[
        fishmeta['starttime'].apply(
            lambda x: 
            not bool(re.match(timeregex, str(x))) 
            if not '-88' else 
            False
        )
    ].index.tolist()
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": badrows_starttime,
        "badcolumn": "starttime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_fish_sample_metadata - starttime format") # tested and working 9nov2021

    # Check: endtime format validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$"
    badrows_endtime = fishmeta[fishmeta['endtime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].index.tolist()
    args.update({
        "dataframe": fishmeta,
        "tablename": "tbl_fish_sample_metadata",
        "badrows": badrows_endtime,
        "badcolumn": "endtime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_fish_sample_metadata - endtime format") # tested and working 9nov2021


    # Check: starttime is before endtime --- crashes when time format is not HH:MM
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    # must be revised
    '''
    df = fishmeta[(fishmeta['starttime'] != '-88') & (fishmeta['endtime'] != '-88') & (fishmeta['endtime'] != -88)]
    print("subset df for time check: ")
    print(df)

    badrows = df[df['starttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False') >= df['endtime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else 'False')].index.tolist()
    if (len(badrows_starttime) == 0 & (len(badrows_endtime) == 0)):
        args.update({
            "dataframe": fishmeta,
            "tablename": "tbl_fish_sample_metadata",
            "badrows": badrows,
            "badcolumn": "starttime",
            "error_message": "Starttime value must be before endtime. Time should be entered in HH:MM format on a 24-hour clock."
            })
        errs = [*errs, checkData(**args)]
        print("check ran - tbl_fish_sample_metadata - starttime before endtime")

    del badrows_starttime
    del badrows_endtime
    '''
    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).strip())

        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    # Removing status part of multicolumn check for now as requested by Jan. 16 Dec 2021
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(fishabud, lu_species, check_cols, lookup_cols)
    
    # Check: multicolumn for species lookup
    args.update({
        "dataframe": fishabud,
        "tablename": "tbl_fish_abundance_data",
        "badrows": badrows,
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check ran - fish_abundance_metadata - multicol species") 

    badrows = multicol_lookup_check(fishdata, lu_species, check_cols, lookup_cols)
    
    # Check: multicolumn for species lookup
    args.update({
        "dataframe": fishdata,
        "tablename": "tbl_fish_length_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })

    errs = [*errs, checkData(**args)]
    print("check ran - fish_length_metadata - multicol species") 

    
    return {'errors': errs, 'warnings': warnings}