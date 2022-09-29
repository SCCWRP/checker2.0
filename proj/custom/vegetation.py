# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic

def vegetation(all_dfs):
    
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
    
    vegmeta = all_dfs['tbl_vegetation_sample_metadata']
    vegdata = all_dfs['tbl_vegetativecover_data']
    epidata = all_dfs['tbl_epifauna_data']
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe":pd.DataFrame({}),
        "tablename":'',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    print("Begin Vegetation Logic Checks..")
    # Note: Vegetation submission should always have vegetativecover_data. Metadata and vegetation data must have corresponding records.
    # Epifauna will sometimes, but not always be submitted. There may be only a subset of epifauna_data or none at all. - confirmed by Jan (26 May 2022)
    # Logic Check 1: sample_metadata & vegetativecover_data
    # Logic Check 1a: vegmeta records not found in vegdata
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows": checkLogic(vegmeta, vegdata, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'], df1_name = "sample_metadata", df2_name = "vegetationcover_data"), 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in sample_metadata must have corresponding records in vegetationcover_data."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records not found in vegetationcover_data") 
    # Logic Check 1b: vegmeta records missing for records provided by vegdata
    # Note: checkLogic() did not output badrows properly for Logic Check 1b. 
    # Bug (checkLogic fcn): if at least one station (=1) has a transreplicate value (ex: 3), then all vegetativecover_data is considered clean but metadata is actually missing the record for some stationno (=2), transectreplicate (=3) BIG BAD
    # Bug fix: merge dfs instead of imported checkLogic function from SMC.
    tmp = vegdata.merge(
        vegmeta.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'],
        how = 'left'
    )
    badrows = tmp[pd.isnull(tmp.present)].index.tolist()

    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": badrows, 
        "badcolumn": "siteid, estuaryname, stationno, samplecollectiondate, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in vegetationcover_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records missing for records provided in vegetationcover_data") 
    del badrows
    # Logic Check 2: epidata records have corresponding sample_metadata records (not vice verse since epifauna data may not always be collected)
    # aka sample_metadata records missing for records provided by epidata
    # checkLogic does not work properly for this df comparison - revised to use same approach as Logic Check 1b
    tmp = epidata.merge(
        vegmeta.assign(present = 'yes'), 
        on = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'transectreplicate'],
        how = 'left'
    )
    badrows = tmp[pd.isnull(tmp.present)].index.tolist()

    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows": badrows,
        "badcolumn": "siteid, estuaryname, samplecollectiondate, stationno, transectreplicate",
        "error_type": "Logic Error",
        "error_message": "Records in epifauna_data must have corresponding records in sample_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - sample_metadata records missing for records provided in epifauna_data") 
    del badrows

    print("End Vegetation Logic Checks..")
    
    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows":vegdata[(vegdata['tallestplantheight_cm']<0) | (vegdata['tallestplantheight_cm'] > 300)].index.tolist(),
        "badcolumn": "tallestplantheight_cm",
        "error_type" : "Value is out of range.",
        "error_message" : "Height should be between 0 to 3 metres"
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < -114.0430560959) | (vegmeta['transectendlongitude'] > -124.5020404709)].index.tolist(),
        "badcolumn": "transectbeginlongitude,transectendlongitude",
        "error_type" : "Value out of range",
        "error_message" : "Your longitude coordinates are outside of california, check your minus sign in your longitude data."
    })
    warnings = [*warnings, checkData(**args)]
    
    args.update({
        "dataframe": vegmeta,
        "tablename": "tbl_vegetation_sample_metadata",
        "badrows":vegmeta[(vegmeta['transectbeginlongitude'] < 32.5008497379) | (vegmeta['transectendlongitude'] > 41.9924715343)].index.tolist(),
        "badcolumn": "transectbeginlatitude,transectendlatitude",
        "error_type" : "Value out of range",
        "error_message" : "Your latitude coordinates are outside of california."
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows":epidata[(epidata['burrows'] == 'Yes') & (epidata['enteredabundance'].apply(lambda x: x < 0))].index.tolist(),
        "badcolumn": "enteredabundance",
        "error_type" : "Value out of range",
        "error_message" : "Your recorded entered abundance value must be greater than 0 and cannot be -88."
    })
    errs = [*errs, checkData(**args)]

    def multicol_lookup_check(df_to_check, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_to_check.columns)), "columns do not exists in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        
        for c in check_cols:
            df_to_check[c] = df_to_check[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_to_check, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_plantspecies;"
    #lookup_sql = f"(SELECT * FROM lu_plantspecies) UNION (SELECT * FROM lu_fishmacrospecies);"
        # will not use union of the lu_lists because vegdata has plantspecies and epidata has fishmacrospecies (two separate multicolumn checks)
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(vegdata, lu_species, check_cols, lookup_cols)
        

    args.update({
        "dataframe": vegdata,
        "tablename": "tbl_vegetativecover_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": "The scientificname/commonname entry did not match the lu_plantspecies lookup list."
                        '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_plantspecies</a>' # need to add href for lu_species
    })

    errs = [*errs, checkData(**args)]
    print("check ran - vegetativecover_data - multicol species") 
    
    del badrows
    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    badrows = multicol_lookup_check(epidata, lu_species, check_cols, lookup_cols)
    args.update({
        "dataframe": epidata,
        "tablename": "tbl_epifauna_data",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lu_fishmacrospecies lookup list.'
                         '<a '
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_plantspecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })

    errs = [*errs, checkData(**args)]
    print("check ran - epifauna_data - multicol species") 

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]


    
    return {'errors': errs, 'warnings': warnings}