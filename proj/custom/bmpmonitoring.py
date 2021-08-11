from copy import deepcopy
from flask import current_app
from inspect import currentframe
from .functions import checkData, get_badrows
import pandas as pd

def monitoring(all_dfs):
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
    
    # This data type should have tbl_ceden_waterquality, tbl_precipitation, and flow
    wq = all_dfs['tbl_ceden_waterquality']
    precip = all_dfs['tbl_precipitation']
    flow = all_dfs['tbl_flow']

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

    # (1) Flow volume must be Liters, ft3, or gallons
    # This applies for bypassvolume as well 
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": 
            flow[
                (~flow['volumeunits'].isin(["L","ft3","gal"]))
                & (~pd.isnull(flow['volumeunits']))
            ].index.tolist()
        ,
        "badcolumn": "volumeunits",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Volume must be reported in L, ft3, or gal"
    })
    warnings = [*warnings, checkData(**args)]

    args.update({
        "badrows": 
            flow[
                (~flow['bypassvolumeunits'].isin(["L","ft3","gal"]))
                & (~pd.isnull(flow['bypassvolumeunits']))
            ].index.tolist()
        ,
        "badcolumn": "bypassvolumeunits",
    })
    warnings = [*warnings, checkData(**args)]

    # (2) Peak Flow Rate should be cubic feet per second
    badrows = flow[
            # first get where it's a non missing value
            (~flow['peakflowrate'].isna()) & flow['peakflowrate'] !=-88
        ][
            # Then get where the units aree not what they should be
            ~flow['peakflowunits'].isin(['cfs'])
        ].index.tolist()
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "peakflowunits",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Peak Flow Units should be cfs (cubic feet per second)"
    })
        

    # (3) Precip Total Depth needs to be cm or inches
    badrows = precip[
            (~precip['totaldepth'].isna()) & precip['totaldepth'] !=-88
        ][
            ~precip['totaldepthunits'].isin(['cm','in'])
        ].index.tolist()

    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "totaldepthunits",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Precipitation Total Depth should be reported in cm or in"
    })
    warnings = [*warnings, checkData(**args)]
        

    # (4) OneHourPeakRateUnit should be  in / hr   or   cm / hr
    badrows = precip[
            (~pd.isnull(precip['onehourpeakrate'])) & precip['onehourpeakrate'] !=-88
        ][
            ~precip['onehourpeakrateunit'].isin(['in/hr','cm/hr','mm/h'])
        ].index.tolist()
        
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "onehourpeakrateunit",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Precipitation OneHourPeakRateUnit should be in/hr, mm/hr, or cm/hr"
    })
    warnings = [*warnings, checkData(**args)]
    
    
    # (5) WQ records need a corresponding record in precip, either in database or current submission
    # Same applies for flow
    unified_precip = pd.concat(
        [
            pd.read_sql("SELECT sitename,eventid from unified_precipitation", current_app.eng),
            precip[['sitename','eventid']]
        ],
        ignore_index = True
    )

    # get sitename eventid pairs that are in wq submission but not the unified precipitation table
    badrows = wq[
        wq.apply(
            lambda row: 
            (row['sitename'], row['eventid']) 
            not in 
            tuple( zip(unified_precip['sitename'], unified_precip['eventid']) )
            ,
            axis = 1
        )
    ].index.tolist()
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_ceden_waterquality',
        "badrows": badrows,
        "badcolumn": "sitename,eventid",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no matching precipitation record for this EventID at this Site"
    })
    errs = [*errs, checkData(**args)]

    # Check for the same thing in the flow table
    badrows = flow[
        flow.apply(
            lambda row: 
            (row['sitename'], row['eventid']) 
            not in 
            tuple( zip(unified_precip['sitename'], unified_precip['eventid']) )
            # might want to store the above tuple in a variable. 
            # It would be interesting to see if pandas calls the functions every time
            # or if they use some kind of caching
            ,
            axis = 1
        )
    ].index.tolist()

    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
    })
    errs = [*errs, checkData(**args)]


    # (6) 
    # Records in their flow table, or water quality table need to have
    # monitoring stations in the unified_monitoringstation table
    # They must be registered as having a measurementtypee of 'Q' for flow and 'WQ' for waterquality
    unified_ms = pd.read_sql(
        "SELECT sitename, stationname FROM unified_monitoringstation WHERE measurementtype = 'Q'",
        current_app.eng
    )
    badrows = flow[
        flow.apply(
            lambda row: 
            (row['sitename'], row['monitoringstation']) 
            not in tuple(zip(unified_ms.sitename, unified_ms.stationname))
            # might want to store the above tuple in a variable. 
            # It would be interesting to see if pandas calls the functions every time
            # or if they use some kind of caching
            ,
            axis = 1
        )  
    ].index.tolist()
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "sitename,monitoringstation",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": f" \
            We were unable to find this monitoring station in the monitoring station table in the database. \
            Either the Monitoring Station data was incorrectly submitted, or it has not yet been submitted. \
            Contact {','.join(current_app.maintainers)} for assistance."
    })
    errs = [*errs, checkData(**args)]

    unified_ms = pd.read_sql(
        "SELECT sitename, stationname FROM unified_monitoringstation WHERE measurementtype = 'WQ'",
        current_app.eng
    )
    badrows = wq[
        wq.apply(
            lambda row: 
            (row['sitename'], row['stationcode']) 
            not in tuple(zip(unified_ms.sitename, unified_ms.stationname))
            # might want to store the above tuple in a variable. 
            # It would be interesting to see if pandas calls the functions every time
            # or if they use some kind of caching
            ,
            axis = 1
        )  
    ].index.tolist()
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_ceden_waterquality',
        "badrows": badrows,
        "badcolumn": "sitename,stationcode",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": f" \
            We were unable to find this monitoring station in the monitoring station table in the database. \
            Either the Monitoring Station data was incorrectly submitted, or it has not yet been submitted. \
            Contact {','.join(current_app.maintainers)} for assistance."
    })
    errs = [*errs, checkData(**args)]

    errs = [e for e in errs if len(e) > 0]
    warnings = [w for w in warnings if len(w) > 0]

    
    return {'errors': errs, 'warnings': warnings}