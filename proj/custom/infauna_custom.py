# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, sample_assignment_check
import re
import pandas as pd


def infauna(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    infaunalabundance = all_dfs['tbl_infaunalabundance']
    infaunalabundance = infaunalabundance.assign(tmp_row = infaunalabundance.index)

    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    eng = g.eng

    badrows = sample_assignment_check(eng = eng, df = infaunalabundance.assign(parameter = 'Infauna'), parameter_column = 'parameter')
    infaunalabundance_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,Lab",
        "error_type": "Logic Error",
        "error_message": f"Your agency is not assigned to submit Infauna for this station (<a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=vw_sample_assignment&datatype=infauna target=_blank>see sample assignments</a>)"
    })
    warnings.append(checkData(**infaunalabundance_args))
    
    
    print("## FORMATTING BUG FIX ##")
    # SampleTime should be written as a string, not a time value. -Jordan 2/19/2019
    infaunalabundance['sampletime'] = infaunalabundance['sampletime'].astype(str)		
    # Jordan - Check that time values for each sheet are in the proper format (hh:mm:ss)
    
    print('Check that SampleTime field is in proper format (e.g. hh:mm:ss)')
    time_format = re.compile('\d{2}:\d{2}:\d{2}')
    # occupation time field
    
    print('Checking SampleTime format...')
    badrows = infaunalabundance[~infaunalabundance.sampletime.str.match(time_format)].tmp_row.tolist()
    print(infaunalabundance[~infaunalabundance.sampletime.str.match(time_format)])
    
    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "SampleTime is not in correct format. Please use the format hh:mm:ss."
    }
    errs = [*errs, checkData(**infaunalabundance_args)]   
    

    print("## LOGIC ##")
    print("Starting Infauna Logic Checks")
    
    #1. Each infaunal abundance record must have a corresponding record in the Sediment Grab Event Table where BenthicInfauna = Yes. Tables matched on StationID and SampleDate.
    print("Each infaunal abundance record must have corresponding record in Sediment Grab Event Table where BenthicInfauna = Yes. Tables matched on StationID and SampleDate")
    gdf = pd.read_sql("SELECT stationid, sampledate, benthicinfauna FROM tbl_grabevent WHERE benthicinfauna = 'Yes'", eng)
    
    # checkLogic on records not found in tbl_grabevent (based on stationID and sampledate)
    print(infaunalabundance[~((infaunalabundance.stationid.isin(gdf.stationid.tolist()))&(infaunalabundance.sampledate.isin(gdf.sampledate.tolist())))])
    badrows = infaunalabundance[
        ~((infaunalabundance.stationid.isin(gdf.stationid.tolist())) & 
        (infaunalabundance.sampledate.isin(gdf.sampledate.tolist())))
    ].tmp_row.tolist()
    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": badrows,
        "badcolumn": "stationid",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no corresponding Sediment Grab Event record (Based on StationID and SampleDate)."
    }
    errs = [*errs, checkData(**infaunalabundance_args)]    
    
    print("END LOGIC CHECKS")


    print("## CUSTOM CHECKS ##")
    #1. If Taxon = NoOrganismsPresent, Then abundance should equal 0.
    print("Custom Check: If Taxon = NoOrganismsPresent, Then abundance should equal 0.")
    print("All records that do not pass this check:")
    print(infaunalabundance[(infaunalabundance.taxon == 'NoOrganismsPresent')&(infaunalabundance.abundance != 0)])
    badrows = infaunalabundance[
        (infaunalabundance.taxon == 'NoOrganismsPresent')&(infaunalabundance.abundance != 0)
    ].tmp_row.tolist()
    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "If Taxon = NoOrganismsPresent, Then abundance should equal 0."
    }
    errs = [*errs, checkData(**infaunalabundance_args)]
    
    
    
    print("## CUSTOM CHECKS ##")
    #1. If abundance == 0, and taxon is not NoOrganismsPresent, then the qualifier should say 'colonial' or 'target analyte present but not counted'
    print("Custom Check: If abundance == 0, and taxon is not NoOrganismsPresent, then the qualifier should say 'colonial' or 'target analyte present but not counted'")
    print("All records that do not pass this check:")
    badrows = infaunalabundance[
        (infaunalabundance.abundance == 0) & 
        (infaunalabundance.taxon != 'NoOrganismsPresent') & 
        (~infaunalabundance.qualifier.isin(['colonial','target analyte present but not counted']))
    ].tmp_row.tolist()
    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "If abundance == 0, and taxon is not NoOrganismsPresent, then the qualifier should say 'colonial' or 'target analyte present but not counted'"
    }
    errs = [*errs, checkData(**infaunalabundance_args)]
    
    
    
    
    #checkData(infaunalabundance[(infaunalabundance.taxon == 'NoOrganismsPresent')&(infaunalabundance.abundance != 0)].tmp_row.tolist(),'Abundance','Undefined Error','error','You recorded Taxon as NoOrganismsPresent. Abundance should equal 0.',infaunalabundance)
    #2. Abundance cannot have -88, must be 1 or greater.
    print("Abundance cannot have -88, must be 0 or greater.")
    print("All records that do not pass this check:")
    print(infaunalabundance[(infaunalabundance.abundance < 0)])
    badrows = infaunalabundance[
        (infaunalabundance.abundance < 0)
    ].tmp_row.tolist()
    infaunalabundance_args = {
        "dataframe": infaunalabundance,
        "tablename": 'tbl_infaunalabundance',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "Abundance should be 0 or greater."
    }
    errs = [*errs, checkData(**infaunalabundance_args)]     
    
    print("## END CUSTOM CHECKS ##")

    return {'errors': errs, 'warnings': warnings}
