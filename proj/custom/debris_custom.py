# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData
import pandas as pd
import datetime

def debris(all_dfs):
    
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

    trawldebris = all_dfs['tbl_trawldebris']
    trawldebris = trawldebris.assign(tmp_row = trawldebris.index)

    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ## LOGIC ##
    print("Starting Debris Logic Checks")
    
    #Jordan - Each debris record must have a corresponding trawl assemblage event record. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number] 
    print("## Each debris record must have a corresponding trawl assemblage event record. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number] ##")
    # call database for trawl assemblage data.
    eng = g.eng
    ta_db = eng.execute("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;")
    ta = pd.DataFrame(ta_db.fetchall())
    if len(ta) > 0:
        ta.columns = ta_db.keys()
        print("tbl_trawlevent records:")
        print(ta)
        # find all records in debris NOT IN trawl assemblage
        invalid_records = trawldebris[
            ~pd.Series(
                zip(
                    trawldebris.stationid,
                    trawldebris.sampledate,
                    trawldebris.samplingorganization,
                    trawldebris.trawlnumber
                )
            ).isin(
                zip(
                    ta.stationid,
                    ta.sampledate,
                    ta.samplingorganization,
                    ta.trawlnumber
                )
            )
        ]
        print('Debris Records Not Found in Trawl Assemblage:')
        print(invalid_records)
        badrows = invalid_records.tmp_row.tolist()
        trawldebris_args = {
            "dataframe": trawldebris,
            "tablename": 'tbl_trawldebris',
            "badrows": badrows,
            "badcolumn": "stationid,sampledate,samplingorganization,trawlnumber",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "Debris data must be submitted after Trawl Assemblage Data."
        }
        errs = [*errs, checkData(**trawldebris_args)]
    else:
        badrows = trawldebris.tmp_row.tolist()
        trawldebris_args = {
            "dataframe": trawldebris,
            "tablename": 'tbl_trawldebris',
            "badrows": badrows,
            "badcolumn": "stationid",
            "error_type": "Undefined Error",
            "is_core_error": False,
            "error_message": "Field data must be submitted before Debris data."
        }
        errs = [*errs, checkData(**trawldebris_args)]
      
    ## END LOGIC CHECKS ##
    print("## END LOGIC CHECKS ##")
    
    ## CUSTOM CHECKS ##
    print("## CUSTOM CHECKS ##")
    print("Starting Debris Custom Checks")

    #Jordan - DebrisType/Comment Check- Required if DebrisType starts with the word "Other" or if DebrisType = 'None'
    print('DebrisType/Comment Check- Required if DebrisType starts with the word Other or if DebrisType = No Debris Present.')
    print(trawldebris[['debristype','comments']])
    print('trawldebris where DebrisType starts with Other, but has no comment:')
    print(trawldebris[(trawldebris.debristype.str.startswith('Other'))&(trawldebris.debristype != 'Other Foliose Algae')&((trawldebris.comments.isnull())|(trawldebris.comments == ''))])
    badrows = trawldebris[
        (trawldebris.debristype.str.startswith('Other')) & 
        (trawldebris.debristype != 'Other Foliose Algae') & 
        (
            (trawldebris.comments.isnull()) | (trawldebris.comments == '')
        )
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "comments",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "DebrisType starts with Other. Comment is Required."
    }
    errs = [*errs, checkData(**trawldebris_args)]
    
    print('trawldebris where DebrisType = No Debris Present, but has no comment:')
    print(trawldebris[(trawldebris.debristype == 'No Debris Present')&((trawldebris.comments.isnull())|(trawldebris.comments == ''))])
    
    badrows = trawldebris[
        (trawldebris.debristype == 'No Debris Present') & 
        ((trawldebris.comments.isnull()) | 
        (trawldebris.comments == ''))
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "comments",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "You have entered a DebrisType of No Debris Present. Comment is Required."
    }
    errs = [*errs, checkData(**trawldebris_args)]
        
    
    #Jordan - DebrisType - Check to see values match lu_debristypes
    print('DebrisType - Check to see values match lu_debristypes')			
    dtypes = eng.execute('select debristype,debrisorigin from lu_debristypes;')
    dt = pd.DataFrame(dtypes.fetchall())
    dt.columns = dtypes.keys()
    
    # compare submitted debristypes to lookuplist
    badrows = trawldebris[
        ~trawldebris.debristype.isin(dt.debristype.tolist())
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "debristype",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": f"DebrisType is invalid please check the list: <a href=/{current_app.script_root}/checker/scraper?action=help&layer=lu_debristypes=_blank>lu_debristypes</a>"
    }
    errs = [*errs, checkData(**trawldebris_args)] 
    
    #Jordan - SampleDate - Check that the sample date falls between 7/1/2023 and 9/30/2023
    print('SampleDate - Check that the sample date falls between 7/1/2023 and 9/30/2023')
    startdate = datetime.datetime(2023,7,1) 
    enddate = datetime.datetime(2023,9,30)
    badrows = trawldebris[
        # The sampledate should never be null - if it somehow magically slipped past core checks, it would flag as an error here
        (trawldebris.sampledate.apply(lambda x: ( pd.Timestamp(x)  < startdate ) if pd.notnull(x) else True)) | 
        (trawldebris.sampledate.apply(lambda x: ( pd.Timestamp(x)  > enddate ) if pd.notnull(x) else True))
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "sampledate",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "SampleDate must be between 7-1-2023 and 9-30-2023"
    }
    errs = [*errs, checkData(**trawldebris_args)] 
    
    #Jordan - Conditional - If DebrisCategory has DebrisOrigin=Natural then EstimateCategory is allowed. If it has DebrisOrigin=Anthropogenic then it should be null.
    print('Conditional - If DebrisCategory has DebrisOrigin=Natural then EstimateCategory is allowed. If it has DebrisOrigin=Anthropogenic then it should be null.')
    debris_origins = trawldebris[['debristype','estimatecategory','tmp_row']]\
        .merge(
            dt, 
            on='debristype',
            how = 'left'
        ).dropna()
    badrows = debris_origins[
        (debris_origins.debrisorigin.str.lower() == 'anthropogenic') & 
        (debris_origins.estimatecategory != 'Not Recorded')
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "estimatecategory",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "DebrisOrigin is Anthropogenic. EstimateCategory should remain null."
    }
    errs = [*errs, checkData(**trawldebris_args)] 
    
    #Jordan - Lookup List - EstimateCategory must be one of  Not Recorded, Moderate or High
    print('Lookup List - If EstimateCategory must be one of Not Recorded, Moderate or High.')
    estimatecategories = ['Not Recorded','Moderate','High']
    badrows = trawldebris[~trawldebris.estimatecategory.isin(estimatecategories)].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "estimatecategory",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "Estimate Category must be Not Recorded, Moderate, or High."
    }
    errs = [*errs, checkData(**trawldebris_args)]    
    
    #Jordan - Conditional - If DebrisCount = -88 then EstimateCategory should be Moderate or High.
    print('Conditional - If DebrisCount = -88 then EstimateCategory should be Moderate or High.')
    badrows = trawldebris[
        (trawldebris.debriscount == -88) & 
        (trawldebris.estimatecategory == 'Not Recorded')
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "estimatecategory",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "DebrisCount = -88. EstimateCategory should be Moderate or High."
    }
    errs = [*errs, checkData(**trawldebris_args)]      
    
    #Jordan - Conditional - If EstimateCategory is Not Recorded, Then DebrisCount should be greater than or equal to 0
    print(' Conditional - If EstimateCategory is Not Recorded, Then DebrisCount should be greater than or equal to 0.')
    badrows = trawldebris[
        (trawldebris.estimatecategory == 'Not Recorded') & 
        (trawldebris.debriscount < 0)
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "debriscount",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "Estimate Category is Not Recorded. DebrisCount must be 0 or greater."
    }
    errs = [*errs, checkData(**trawldebris_args)]      
    
    
    #Jordan - Lookup list - Link data to Bight station list to look for mismatched records.
    print('Lookup list - Link data to Bight station list to look for mismatched records.')
    bight_stations = eng.execute("""SELECT stationid,assigned_agency AS trawlagency FROM field_assignment_table WHERE "parameter" = 'trawl';""")
    bs = pd.DataFrame(bight_stations.fetchall())
    bs.columns = bight_stations.keys()
    false_records = trawldebris[
        ['stationid','samplingorganization']
    ].apply(
        lambda x: False 
        if tuple([x.stationid,x.samplingorganization]) in zip(bs.stationid,bs.trawlagency) 
        else True,
        axis=1
    )
    badrows = trawldebris[false_records == True].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "stationid,samplingorganization",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message": "StationID/SamplingOrganization submitted not a Bight StationID/SamplingOrganization."
    }
    warnings = [*warnings, checkData(**trawldebris_args)]      
    
    #Jordan - Conditional - If DebrisType = 'No Debris Present' Then DebrisCount = 0
    print('Conditional - If DebrisType = No Debris Present Then DebrisCount = 0')
    print(trawldebris[(trawldebris.debristype == 'No Debris Present')&(trawldebris.debriscount != 0)])
    badrows = trawldebris[
        (trawldebris.debristype == 'No Debris Present') & 
        (trawldebris.debriscount != 0)
    ].tmp_row.tolist()
    trawldebris_args = {
        "dataframe": trawldebris,
        "tablename": 'tbl_trawldebris',
        "badrows": badrows,
        "badcolumn": "debriscount",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message": "You have entered a DebrisType of No Debris Present. DebrisCount must equal 0."
    }
    errs = [*errs, checkData(**trawldebris_args)] 
    
    ## END CUSTOM CHECKS ##
    print("## END CUSTOM CHECKS ##")

    return {'errors': errs, 'warnings': warnings}
