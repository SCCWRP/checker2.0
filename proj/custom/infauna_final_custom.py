# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app
from .functions import checkData
import re

def infauna_final(all_dfs):
    
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


    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    infaunalabundance_final = all_dfs['tbl_infaunalabundance_final']


    infaunalabundance_final_args = {
        "dataframe": infaunalabundance_final,
        "tablename": 'tbl_infaunalabundance_final',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    eng = g.eng
    
    print("## FORMATTING BUG FIX ##")
    # SampleTime should be written as a string, not a time value. -Jordan 2/19/2019
    infaunalabundance_final['sampletime'] = infaunalabundance_final['sampletime'].astype(str)		
    # Jordan - Check that time values for each sheet are in the proper format (hh:mm:ss)
    print('Check that SampleTime field is in proper format (e.g. hh:mm:ss)')
    print('Check that SampleTime field is in proper format (e.g. hh:mm:ss)')
    time_format = re.compile('\d{2}:\d{2}:\d{2}')
    # occupation time field
    print('Checking SampleTime format...')
    print('Checking SampleTime format...')
    print(infaunalabundance_final[~infaunalabundance_final.sampletime.str.match(time_format)])
    badrows = infaunalabundance_final[
        ~infaunalabundance_final.sampletime.str.match(time_format)
    ].tmp_row.tolist()
    infaunalabundance_reid_args = {
        "dataframe": infaunalabundance_final,
        "tablename": 'tbl_infaunalabundance_final',
        "badrows": badrows,
        "badcolumn": "sampletime",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "SampleTime is not in correct format. Please use the format hh:mm:ss."
    }
    errs = [*errs, checkData(**infaunalabundance_reid_args)]   


    print("## LOGIC ##")
    print("Starting Infauna Logic Checks")
    #1. Each infaunal abundance record must have a corresponding record in the Sediment Grab Event Table where BenthicInfauna = Yes. Tables matched on StationID and SampleDate.
    print("Each infaunal abundance record must have corresponding record in Sediment Grab Event Table where BenthicInfauna = Yes. Tables matched on StationID and SampleDate")
    grab_event_sql = "SELECT stationid, sampledate, benthicinfauna FROM tbl_grabevent WHERE benthicinfauna = 'Yes' ;"
    grab_infauna_records = eng.execute(grab_event_sql)
    gdf = pd.DataFrame(grab_infauna_records.fetchall());
    gdf.columns = grab_infauna_records.keys()
    # checkLogic on records not found in tbl_grabevent (based on stationID and sampledate)
    print(infaunalabundance_final[~((infaunalabundance_final.stationid.isin(gdf.stationid.tolist()))&(infaunalabundance_final.sampledate.isin(gdf.sampledate.tolist())))])
    badrows = infaunalabundance_final[
        ~((infaunalabundance_final.stationid.isin(gdf.stationid.tolist())) & 
        (infaunalabundance_final.sampledate.isin(gdf.sampledate.tolist())))
    ].tmp_row.tolist()
    infaunalabundance_reid_args = {
        "dataframe": infaunalabundance_final,
        "tablename": 'tbl_infaunalabundance_final',
        "badrows": badrows,
        "badcolumn": "stationid",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no corresponding Sediment Grab Event record (Based on StationID and SampleDate)."
    }
    errs = [*errs, checkData(**infaunalabundance_reid_args)]    

    print("END LOGIC CHECKS")


    print("## CUSTOM CHECKS ##")
    #1. If Taxon = NoOrganismsPresent, Then abundance should equal 0.
    print("Custom Check: If Taxon = NoOrganismsPresent, Then abundance should equal 0.")
    print("All records that do not pass this check:")
    print(infaunalabundance_final[(infaunalabundance_final.taxon == 'NoOrganismsPresent')&(infaunalabundance_final.abundance != 0)])
    badrows = infaunalabundance_final[
        (infaunalabundance_final.taxon == 'NoOrganismsPresent')&(infaunalabundance_final.abundance != 0)
    ].tmp_row.tolist()
    infaunalabundance_final_args = {
        "dataframe": infaunalabundance_final,
        "tablename": 'tbl_infaunalabundance_final',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "If Taxon = NoOrganismsPresent, Then abundance should equal 0."
    }
    errs = [*errs, checkData(**infaunalabundance_final_args)]
    
    #checkData(infaunalabundance_final[(infaunalabundance_final.taxon == 'NoOrganismsPresent')&(infaunalabundance_final.abundance != 0)].tmp_row.tolist(),'Abundance','Undefined Error','error','You recorded Taxon as NoOrganismsPresent. Abundance should equal 0.',infaunalabundance_final)
    #2. Abundance cannot have -88, must be 1 or greater.
    print("Abundance cannot have -88, must be 1 or greater.")
    print("All records that do not pass this check:")
    print(infaunalabundance_final[(infaunalabundance_final.abundance < 1)])
    badrows = infaunalabundance_final[(infaunalabundance_final.abundance < 1)].tmp_row.tolist()
    infaunalabundance_reid_args = {
        "dataframe": infaunalabundance_final,
        "tablename": 'tbl_infaunalabundance_final',
        "badrows": badrows,
        "badcolumn": "abundance",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "Abundance should be 1 or greater."
    }
    errs = [*errs, checkData(**infaunalabundance_reid_args)]     

    print("## END CUSTOM CHECKS ##")



    return {'errors': errs, 'warnings': warnings}
