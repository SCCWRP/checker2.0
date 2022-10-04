# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from .functions import checkData, get_badrows, checkLogic
import re

def crabtrap(all_dfs):
    
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
    
    crabmeta = all_dfs['tbl_crabtrap_metadata']
    crabinvert = all_dfs['tbl_crabfishinvert_abundance']
    crabmass = all_dfs['tbl_crabbiomass_length']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    #commented out since df is a placeholder variable for DataFrame

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
    print("Begin Crab Trap Logic Checks...")
    # Logic Check 1: crabtrap_metadata & crabfishinvert_abundance
    # Logic Check 1a: crabmeta records not found in crabinvert
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": checkLogic(crabmeta, crabinvert, cols = ['siteid', 'estuaryname', 'traptype', 'traplocation', 'stationno', 'replicate'], df1_name = "Crabtrap_metadata", df2_name = "Crab_fish_invert_abundance_data"), 
        "badcolumn": "siteid, estuaryname, traptype, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crabtrap_metadata must have corresponding records in Crab_fish_invert_abundance."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - crabtrap_metadata records not found in crabfishinvert_abundance") 
    # Logic Check 1b: crabmeta records missing for records provided by crabinvert
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": checkLogic(crabinvert, crabmeta, cols = ['siteid', 'estuaryname', 'traptype', 'traplocation', 'stationno', 'replicate'], df1_name = "Crab_fish_invert_abundance_data", df2_name = "Crabtrap_metadata"), 
        "badcolumn": "siteid, estuaryname, traptype, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crab_fish_invert_abundance must have corresponding records in Crabtrap_metadata."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - crabtrap_metadata records missing for records provided in crabfishinvert_abundance") 

    # Logic Check 2: crabfishinvert_abundance & crabbiomass_length
    # Logic Check 2a: crabinvert records not found in crabmass
    # changed order of colnames based off template rather than pkey from db
    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": checkLogic(crabinvert, crabmass, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'traptype', 'traplocation', 'replicate', 'scientificname'], df1_name = "Crab_fish_invert_abundance_data", df2_name = "Crab_biomass_length_data"), 
        "badcolumn": "siteid, estuaryname, samplecollectiondate, traptype, scientificname, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crab_fish_invert_abundance must have corresponding records in Crab_biomass_length."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - crabfishinvert_abundance records not found in crabbiomass_length")
    # Logic Check 1b: crabinvert records missing for records provided by crabmass
    # changed ordering of colnames off template rather than db pkey fields
    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": checkLogic(crabmass, crabinvert, cols = ['siteid', 'estuaryname', 'stationno', 'samplecollectiondate', 'traptype', 'traplocation', 'replicate', 'scientificname'], df1_name = "Crab_biomass_length_data", df2_name = "Crab_fish_invert_abundance_data"), 
        "badcolumn": "siteid, estuaryname, samplecollectiondate, traptype, scientificname, traplocation, stationno, replicate",
        "error_type": "Logic Error",
        "error_message": "Records in Crab_biomass_length must have corresponding records in Crab_fish_invert_abundance."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - logic - crabinvert records missing for records provided in crabmass") 

    print("End Crab Trap Logic Checks...")

    ''' disabled check by Paul - doesn't make any sessions should be a combination of deployment date/time vs retrieve date/time - 3dec2021
    # Check: starttime format validation
    timeregex = "([01]?[0-9]|2[0-3]):[0-5][0-9]$" #24 hour clock HH:MM time validation
    # replacing null with -88 since data has been filled w/ -88
    #badrows_deploymenttime = crabmeta[crabmeta['deploymenttime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not pd.isnull(x) else False)].index.tolist()
    badrows_deploymenttime = crabmeta[crabmeta['deploymenttime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].index.tolist()
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": badrows_deploymenttime,
        "badcolumn": "deploymenttime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrapmeta_metadata - deploymenttime format") 

    badrows_retrievaltime = crabmeta[crabmeta['retrievaltime'].apply(lambda x: not bool(re.match(timeregex, str(x))) if not '-88' else False)].index.tolist()
    args.update({
        "dataframe": crabmeta,
        "tablename": "tbl_crabtrap_metadata",
        "badrows": badrows_retrievaltime,
        "badcolumn": "retrievaltime",
        "error_message": "Time should be entered in HH:MM format on a 24-hour clock."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrapmeta_metadata - retrievaltime format") 

    # Check: starttime is before endtime --- crashes when time format is not HH:MM
    # Note: starttime and endtime format checks must pass before entering the starttime before endtime check
    if (len(badrows_deploymenttime) == 0 & (len(badrows_retrievaltime) == 0)):
        args.update({
            "dataframe": crabmeta,
            "tablename": "tbl_crabtrap_metadata",
            "badrows": crabmeta[crabmeta['deploymenttime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else '') >= crabmeta['retrievaltime'].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not '-88' else '')].index.tolist(),
            "badcolumn": "deploymenttime",
            "error_message": "Deploymenttime value must be before retrievaltime. Time should be entered in HH:MM format on a 24-hour clock."
            })
        errs = [*errs, checkData(**args)]
        print("check ran - tbl_crabtrap_metadata - deploymenttime before retrievaltime")

    del badrows_deploymenttime
    del badrows_retrievaltime
    ''' 
    '''
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        # I changed badrows to 'deploymenttime > retrivaltime' because of the error_message
        "badrows":crabmeta[crabmeta.deploymenttime > crabmeta.retrievaltime].index.tolist(),
        "badcolumn":"deploymenttime,retrievaltime",
        "error_type": "Date Value out of range",
        "error_message" : "Deployment time should be before retrieval time."
    })
    errs = [*errs, checkData(**args)]
    print('Finished: Compare deployment time to retrieval time')
    '''


    print("enter abundance check")
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        #"badrows":crabinvert[((crabinvert['abundance'] < 0) | (crabinvert['abundance'] > 100) & (crabinvert['abundance'] != -88)].index.tolist(),
        "badrows": crabinvert[crabinvert['abundance'].apply(lambda x: ((x < 0) | (x > 100)) & (x != -88))].index.tolist(),
        "badcolumn": "abundance",
        "error_type": "Value out of range",
        "error_message": "Your abundance value must be between 0 to 100."
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabfishinvert_abundance - abundance check") 

    # New checks written by Duy - 2022-10-04
    
    # Check 1: if trapsuccess = yes in meta, then needs to be value for catch either yes or no
    badrows = crabmeta[
        (crabmeta['trapsuccess'].apply(lambda x: str(x).strip()) == 'yes') & (crabmeta['catch'].apply(lambda x: str(x).strip()) not in ['yes','no'])
    ].index.tolist()  
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        "badrows": badrows,
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess = yes, then needs to be value for catch (either yes or no)"
    })
    errs = [*errs, checkData(**args)]
    
    # Check 2: if trapsuccess = no in meta, catch = Null
    badrows = crabmeta[
        (crabmeta['trapsuccess'].apply(lambda x: str(x).strip()) == 'no') & (not pd.isnull(crabmeta['catch']))
    ].index.tolist()  
    args.update({
        "dataframe": crabmeta,
        "tablename": 'tbl_crabtrap_metadata',
        "badrows": badrows,
        "badcolumn": "catch",
        "error_type": "Undefined Error",
        "error_message": "If trapsuccess = no, catch needs to be empty"
    })
    errs = [*errs, checkData(**args)]
    
    # Check 3: If catch = No in meta, then abundance = 0 in abundance tab
    merged= pd.merge(
        crabinvert,
        crabmeta, 
        how='left',
        suffixes=('_abundance', '_meta'),
        on = ['siteid','estuaryname','traptype','traplocation','stationno','replicate']
    )

    badrows = merged[(merged['catch'] == 'no') & (merged['abundance'] > 0)].index.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = No in crab_meta, then abundance shoud be 0 in invert_abundance tab"
    })
    errs = [*errs, checkData(**args)]
    
    # Check 4: If catch = Yes in meta, then abundance is non-zero integer in abundance tab
    badrows = merged[(merged['catch'] == 'yes') & (merged['abundance'] <= 0)].index.tolist()
    args.update({
        "dataframe": crabinvert,
        "tablename": 'tbl_crabfishinvert_abundance',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "error_message": "If catch = Yes in crab_meta, then abundance should be a non-zero integer in abundance tab."
    })
    errs = [*errs, checkData(**args)]

    print("before multicol check")
    def multicol_lookup_check(df_tocheck, lookup_df, check_cols, lookup_cols):
        assert set(check_cols).issubset(set(df_tocheck.columns)), "columns do not exist in the dataframe"
        assert isinstance(lookup_cols, list), "lookup columns is not a list"

        lookup_df = lookup_df.assign(match="yes")
        df_tocheck['status'] = df_tocheck['status'].astype(str)
        
        for c in check_cols:
            df_tocheck[c] = df_tocheck[c].apply(lambda x: str(x).lower().strip())
        for c in lookup_cols:
            lookup_df[c] = lookup_df[c].apply(lambda x: str(x).lower().strip())

        merged = pd.merge(df_tocheck, lookup_df, how="left", left_on=check_cols, right_on=lookup_cols)
        badrows = merged[pd.isnull(merged.match)].index.tolist()
        return(badrows)

    lookup_sql = f"SELECT * FROM lu_fishmacrospecies;"
    lu_species = pd.read_sql(lookup_sql, g.eng)
    #check_cols = ['scientificname', 'commonname', 'status']
    check_cols = ['scientificname', 'commonname']
    #lookup_cols = ['scientificname', 'commonname', 'status']
    lookup_cols = ['scientificname', 'commonname']

    badrows = multicol_lookup_check(crabinvert,lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": crabinvert,
        "tablename": "tbl_crabfishinvert_abundance",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": "The scientificname/commonname entry did not match the lookup list."
                        '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species
    })
    errs = [*errs, checkData(**args)]
    print("check ran - crabfishinvert_abundance - multicol species") 
    badrows = multicol_lookup_check(crabmass, lu_species, check_cols, lookup_cols)

    args.update({
        "dataframe": crabmass,
        "tablename": "tbl_crabbiomass_length",
        "badrows": badrows,
        "badcolumn": "commonname",
        "error_type": "Multicolumn Lookup Error",
        "error_message": f'The scientificname/commonname entry did not match the lookup list.'
                         '<a ' 
                        f'href="/{lu_list_script_root}/scraper?action=help&layer=lu_fishmacrospecies" '
                        'target="_blank">lu_fishmacrospecies</a>' # need to add href for lu_species

    })
    errs = [*errs, checkData(**args)]
    print("check ran - crabbiomass_length - multicol species") 
    # Disabling Time Check for Crab Trap to submit data.
    '''
    # Crab Trap Time Logic Check (Mina N.).....currently a work in progress
    ddate = crabmeta[crabmeta['deploymentdate']].dt.strftime('%Y-%m-%d')
    print(ddate)
    rdate = crabmeta[crabmeta['retrievaldate']].dt.strftime('%Y-%m-%d')
    dtime = crabmeta[crabmeta['deploymenttime']].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not pd.isnull(x) else "00:00:00")
    rtime = crabmeta[crabmeta['retrievaltime']].apply(lambda x: pd.Timestamp(str(x)).strftime('%H:%M') if not pd.isnull(x) else "00:00:00")

    # Merge date columns with respectvie time columns:
    deployment = ddate + ' ' + dtime
    ret = rdate + ' ' + rtime
    deployment = pd.to_datetime(deployment)
    retrieval = pd.to_datetime(ret)

    # Return rows if deployment is after retrieval
    badrows = crabmeta[crabmeta[deployment]][deployment > retrieval]

    args.update({
     "dataframe": crabmeta,
     "tablename": "tbl_crabtrap_metadata",
     "badrows": badrows,
     "badcolumn": "deploymentdate,deploymenttime,retrievaldate,retrievaltime",
     "error_message": "Values in 'deployment time' column is before values in 'retrieval time' column"
    
    })
    errs = [*errs, checkData(**args)]
    print("check ran - tbl_crabtrap_metadata - time logic check")
    '''

    return {'errors': errs, 'warnings': warnings}
