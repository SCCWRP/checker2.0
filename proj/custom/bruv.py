

from inspect import currentframe
import pandas as pd
from flask import current_app, g
from .functions import checkData, get_badrows

#define new function called 'bruvlab' for lab data dataset - 
def bruv(all_dfs):
    
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
    
    # These are the dataframes that got submitted for bruv
    protocol = all_dfs['tbl_protocol_metadata']
    bruvmeta = all_dfs['tbl_bruv_metadata']
    #bruvdata = all_dfs['tbl_bruv_data'] #leaving tbl_bruv_data out for later, this is lab data and will not be submitted with the metadata tables - Zaib

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # Im just initializing the args dictionary
    
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
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    
    #(1) maxnspecies is nonnegative
    '''
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata['maxnspecies'] < 0].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        #"error_message":"Max number of species should be between 0 and 100"
        "error_message":"Max number of species must be nonnegative."
    })
    errs = [*errs, checkData(**args)]

    #(2) maxnspecies should not exceed 100 (warning)
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[(bruvdata['maxnspecies'] < 0) | (bruvdata['maxnspecies'] > 100)].index.tolist(),
        "badcolumn":"maxnspecies",
        "error_type":"Value out of range",
        "error_message":"Max number of species should NOT exceed 100."
    })
    warnings = [*warnings, checkData(**args)]

    
    args.update({
        "dataframe":bruvdata,
        "tablename":'tbl_bruv_data',
        "badrows":bruvdata[bruvdata.foventeredtime.apply(pd.Timestamp) > bruvdata.fovlefttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn":"foventeredtime,fovlefttime",
        "error_type": "Value out of range",
        "error_message":"FOV entered time must be before FOV left time"
    })
    errs = [*errs, checkData(**args)]
    '''
    
    #(1) tbl_bruv_metadata - time format check HH:MM AM
    ## commenting out time checks for now - need to check in with Jan - Zaib 
    '''
    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[bruvmeta.bruvintime.apply(pd.Timestamp) > bruvmeta.bruvouttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "bruvintime,bruvouttime",
        "error_type" : "Time format out of range",
        "error_message" : "Bruvintime must be before bruvouttime"
    })
    errs = [*errs, checkData(**args)]
    
    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[bruvmeta.bruvintime.apply(pd.Timestamp) > bruvmeta.bruvouttime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "bruvintime,bruvouttime",
        "error_type" : "Time format out of range",
        "error_message" : "Bruvintime must be before bruvouttime"
    })
    errs = [*errs, checkData(**args)]
    '''
    #() depth_m is positive for tbl_bruv_metadata 
    args.update({
        "dataframe": bruvmeta,
        "tablename": 'tbl_bruv_metadata',
        "badrows": bruvmeta[(bruvmeta['depth_m'] < 0) & (bruvmeta['depth_m'] != -88)].index.tolist(),
        "badcolumn": "depth_m",
        "error_type" : "Value out of range",
        "error_message" : "Depth measurement should not be a negative number, must be greater than 0."
    })
    errs = [*warnings, checkData(**args)]

    #tbl_bruv_data will have the species column check, yet to be tested
    def multicol_lookup_check(df_to_check,lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    '''
    lookup_sql = f"SELECT * from lu_fishmacroplantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname', 'status']

    badrows = multicol_lookup_check(bruvdata, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": bruvdata,
        "tablename": "tbl_bruv_data",
        "badrows": badrows,
        "badcolumn": "scientificname",
        "error_type" : "Multicolumn Lookup Error",
        "error_message" : "The scientificname/commonname/status entry did not match the lookup list." # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    '''

    return {'errors': errs, 'warnings': warnings}
    