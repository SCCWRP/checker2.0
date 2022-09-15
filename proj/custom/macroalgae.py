# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic

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

    # generalizing multicol_lookup_check
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

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]
    print("Begin Macroalgae Multicol Checks for matching SiteID to EstuaryName...")
    lookup_sql = f"SELECT * FROM lu_siteid;"
    lu_siteid = pd.read_sql(lookup_sql, g.eng)
    check_cols = ['siteid','estuaryname']
    lookup_cols = ['siteid','estuary']
    # Multicol - algaemeta
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": multicol_lookup_check(algaemeta,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaemeta")
    errs = [*errs, checkData(**args)]
    # Multicol - algaecover
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": multicol_lookup_check(algaecover,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaecover")
    errs = [*errs, checkData(**args)]
    # Multicol - algaefloating
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": multicol_lookup_check(algaefloating,lu_siteid, check_cols, lookup_cols),
        "badcolumn":"siteid, estuaryname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The siteid/estuaryname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_siteid" '
                        'target="_blank">lu_siteid</a>'
        
    })
    print("check ran - multicol lookup, siteid and estuaryname - algaefloating")
    errs = [*errs, checkData(**args)]

    print("End Macroalgae Multicol Checks for matching SiteID to EstuaryName...")

    # Check: transectreplicate must be positive or -88 for tbl_macroalgae_sample_metadata
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[(algaemeta['transectreplicate'] <= 0) & (algaemeta['transectreplicate'] != -88)].index.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive transectreplicate - algaemeta")

    # Check: transectreplicate must be positive or -88 for tbl_algaecover_data
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['transectreplicate'] <= 0) & (algaecover['transectreplicate'] != -88)].index.tolist(),
        "badcolumn": "transectreplicate",
        "error_type" : "Value Error",
        "error_message" : "TransectReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive transectreplicate - algaecover")
    # Check: plotreplicate must be positive or -88 for tbl_algaecover_data
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['plotreplicate'] <= 0) & (algaecover['plotreplicate'] != -88)].index.tolist(),
        "badcolumn": "plotreplicate",
        "error_type" : "Value Error",
        "error_message" : "PlotReplicate must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive plotreplicate - algaecover")

    # transectlength_m must be postive (> 0)
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": algaemeta[algaemeta['transect_length_m'] <= 0].index.tolist(),
        "badcolumn": "transect_length_m",
        "error_type" : "Value out of range",
        "error_message" : "Transect length must be greater than 0."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - positive transect_length_m - algaemeta")
    
   # return {'errors': errs, 'warnings': warnings}
    print("Begin Macroalgae Logic Checks...")
    # Logic Check 1: sample_metadata & algaecover_data
    # Logic Check 1a: algaemeta records not found in algaecover
    args.update({
        "dataframe": algaemeta,
        "tablename": "tbl_macroalgae_sample_metadata",
        "badrows": checkLogic(algaemeta, algaecover, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'], df1_name = "sample_metadata", df2_name = "Algaecover_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in Algaecover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records not found in algaecover_data") 
    # Logic Check 1b: algaemeta records missing for records provided by algaecover
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": checkLogic(algaecover, algaemeta, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'], df1_name = "Algaecover_data", df2_name = "sample_metadata"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in Algaecover_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records missing for records provided in algaecover_data") 


    print("End Macroalgae Logic Checks...")

    # CoverType & Species Check: if covertype is plant, then scientificname CANNOT be 'Not recorded'
    # if covertype is not plant, then scientificname can be 'Not recorded' - no check needs to be written for this one
    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": algaecover[(algaecover['covertype'] == 'plant') & (algaecover['scientificname'] == 'Not recorded')].index.tolist(), 
        "badcolumn": "covertype, scientificname",
        "error_type": "Value Error",
        "error_message": "CoverType is 'plant' so the ScientificName must be a value other than 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - algaecover_data - covertype is plant, sciname must be an actual plant") 

    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(algaecover, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": algaecover,
        "tablename": "tbl_algaecover_data",
        "badrows": badrows,
        "badcolumn":"commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
        
    })

    errs = [*errs, checkData(**args)]
    print("check ran - algeacover_data - multicol species") 

    # ALGAE FLOATING DATA CHECKS
    # EstimatedCover & ScientificName Check: if estimatedcover is 0, then scientificname MUST be 'Not recorded'
    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": algaefloating[(algaefloating['estimatedcover'] == 0) & (algaefloating['scientificname'] != 'Not recorded')].index.tolist(), 
        "badcolumn": "estimatedcover, scientificname",
        "error_type": "Value Error",
        "error_message": "EstimatedCover is 0. The ScientificName MUST be 'Not recorded'."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - floating_data - estimatedcover is 0, sciname must be NR") 

    badrows = multicol_lookup_check(algaefloating, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": algaefloating,
        "tablename": "tbl_floating_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list '
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species

    })

    errs = [*errs, checkData(**args)]
    print("check ran - floating_data - multicol species") 

    
    return {'errors': errs, 'warnings': warnings}

'''
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
'''