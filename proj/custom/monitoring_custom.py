from copy import deepcopy
from flask import current_app, g
from inspect import currentframe
from .functions import checkData, mismatch, check_time_format
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
    
    # This data type should have tbl_waterquality, tbl_precipitation, and flow
    wq = all_dfs['tbl_waterquality']
    precip = all_dfs['tbl_precipitation']
    flow = all_dfs['tbl_flow']
    
    
    # add tmp_row column - a copy of the index which ensures accuracy of assigning errors to rows
    wq['tmp_row'] = wq.index
    precip['tmp_row'] = precip.index
    flow['tmp_row'] = flow.index

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

    # database connection - readonly
    eng = g.readonly_eng

    print("# ----------------------                         Logic Checks                                    ----------------------- #")
    ##############################################################################################################################
    # ----------------------                         Logic Checks                                    --------------------------- #
    ##############################################################################################################################


    # CHECK - WQ and Flow records need a corresponding record in precip, either in database or current submission
    # Same applies for flow
    print("# CHECK - WQ and Flow records need a corresponding record in precip, either in database or current submission")
    print("(Same applies for flow)")

    print("Query database and get combined sitename, eventid pairs and combining with current submission")
    combined_precip = pd.concat(
        [
            pd.read_sql("SELECT sitename,eventid from tbl_precipitation", eng),
            precip[['sitename','eventid']]
        ],
        ignore_index = True
    )
    print("DONE - Querying database and get combined sitename, eventid pairs and combining with current submission")

    # get sitename eventid pairs that are in wq submission but not the (tbl) precipitation table
    print("# get sitename eventid pairs that are in wq submission but not the (tbl) precipitation table")
    badrows = mismatch(wq, combined_precip, mergecols = ['sitename','eventid'])
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "sitename,eventid",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no matching precipitation record for this EventID at this Site"
    })
    errs = [*errs, checkData(**args)]

    # Check the same thing in the flow table
    print("# Check the same thing in the flow table")
    badrows = mismatch(flow, combined_precip, mergecols = ['sitename','eventid'], row_identifier = 'tmp_row')
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
    })
    errs = [*errs, checkData(**args)]
    
    # END CHECK - WQ and Flow records need a corresponding record in precip, either in database or current submission
    # Same applies for flow
    print("# END CHECK - WQ and Flow records need a corresponding record in precip, either in database or current submission")






    # CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the water quality table
    print("# CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the water quality table")

    # get sitename eventid pairs that are in precip submission but not in the water quality table
    print("# get sitename eventid pairs that are in precip submission but not in the water quality table")
    badrows = mismatch(precip, wq, mergecols=['sitename','eventid'])
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "sitename,eventid",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "There is no matching water quality record for this EventID at this Site"
    })
    warnings = [*warnings, checkData(**args)]

    print("# END CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the water quality table")

    # CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the flow table
    print("# CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the flow table")

    # get sitename eventid pairs that are in precip submission but not in the flow table
    print("# get sitename eventid pairs that are in precip submission but not in the flow table")
    badrows = mismatch(precip, flow, mergecols=['sitename','eventid'])
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "sitename,eventid",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "There is no matching flow record for this EventID at this Site"
    })
    warnings = [*warnings, checkData(**args)]

    print("# END CHECK - Issue a warning if there is a record in the precipitation table that does not have a corresponding record in the flow table")





    # CHECK - Monitoringstation in Precipitation tab must match corresponding SiteName, and it must have a measurementtype of "P" (Precipitation)
    print("# CHECK - Monitoringstation in Precipitation tab must match corresponding SiteName, and it must have a measurementtype of 'P' (Precipitation)")
    unified_ms = pd.read_sql("SELECT sitename, stationname AS monitoringstation FROM unified_monitoringstation WHERE measurementtype = 'P'", eng)
    
    badrows = mismatch(precip, unified_ms, mergecols=['sitename', 'monitoringstation'], row_identifier='tmp_row')
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "sitename,monitoringstation",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "This Monitoring Station name was not found in the Monitoring Station table for this Site (Was it registered as a Rain Gauge?)"
    })
    errs = [*errs, checkData(**args)]
    
    # END CHECK - Monitoringstation in Precipitation tab must match corresponding SiteName, and it must have a measurementtype of "P" (Precipitation)
    print("# END CHECK - Monitoringstation in Precipitation tab must match corresponding SiteName, and it must have a measurementtype of 'P' (Precipitation)")





    # CHECK - Monitoringstation in Flow tab must match corresponding SiteName and BMPName, and it must have a measurementtype of "Q" (Flow)
    print("# CHECK - Monitoringstation in Flow tab must match corresponding SiteName and BMPName, and it must have a measurementtype of 'Q' (Flow)")
    unified_ms = pd.read_sql("SELECT sitename, bmpname, stationname AS monitoringstation FROM unified_monitoringstation WHERE measurementtype = 'Q'", eng)
    
    badrows = mismatch(flow, unified_ms, mergecols=['sitename', 'bmpname', 'monitoringstation'], row_identifier='tmp_row')
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "sitename,bmpname,monitoringstation",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "This Monitoring Station name was not found in the Monitoring Station table for this Site and BMP (It may be assigned to the wrong BMP, or not correctly registered as a Flow Station)"
    })
    errs = [*errs, checkData(**args)]
    
    # END CHECK - Monitoringstation in Flow tab must match corresponding SiteName and BMPName, and it must have a measurementtype of "Q" (Flow)
    print("# END CHECK - Monitoringstation in Flow tab must match corresponding SiteName and BMPName, and it must have a measurementtype of 'Q' (Flow)")






    # CHECK - Monitoringstation in WQ tab must match corresponding SiteName and BMPName, and it must have a measurementtype of "WQ" (Water Quality)
    print("# CHECK - Monitoringstation in WQ tab must match corresponding SiteName and BMPName, and it must have a measurementtype of 'WQ' (Water Quality)")
    unified_ms = pd.read_sql("SELECT sitename, bmpname, stationname AS stationcode FROM unified_monitoringstation WHERE measurementtype = 'WQ'", eng)
    
    badrows = mismatch(wq, unified_ms, mergecols=['sitename', 'bmpname', 'stationcode'], row_identifier='tmp_row')
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "sitename,bmpname,stationcode",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "This Monitoring Station name was not found in the Monitoring Station table for this Site and BMP (It may be assigned to the wrong BMP, or not correctly registered as a Water Quality Station)"
    })
    errs = [*errs, checkData(**args)]
    
    # END CHECK - Monitoringstation in WQ tab must match corresponding SiteName and BMPName, and it must have a measurementtype of "WQ" (Water Quality)
    print("# END CHECK - Monitoringstation in WQ tab must match corresponding SiteName and BMPName, and it must have a measurementtype of 'WQ' (Water Quality)")





    ##############################################################################################################################
    # ----------------------                         END Logic Checks                                    ----------------------- #
    ##############################################################################################################################
    print("# ----------------------                         END Logic Checks                                    ----------------------- #")
    
    



    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- # 





    print("# ----------------------                         Precipitation Checks                                    ----------------------- #")
    ######################################################################################################################################
    # ----------------------                         Precipitation Checks                                    --------------------------- #
    ######################################################################################################################################


    
    # CHECK - if totaldepth is less than zero, it must be -88
    print("# CHECK - if totaldepth is less than zero, it must be -88")

    badrows = precip[
        (precip['totaldepth'] < 0) & (precip['totaldepth'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "totaldepth",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If totaldepth is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if totaldepth is less than zero, it must be -88
    print("# END CHECK - if totaldepth is less than zero, it must be -88")
    



    # CHECK - if onehourpeakintensity is less than zero, it must be -88
    print("# CHECK - if onehourpeakintensity is less than zero, it must be -88")

    badrows = precip[
        (precip['onehourpeakintensity'] < 0) & (precip['onehourpeakintensity'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "onehourpeakintensity",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If onehourpeakintensity is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if onehourpeakintensity is less than zero, it must be -88
    print("# END CHECK - if onehourpeakintensity is less than zero, it must be -88")



       

    
    # CHECK - if antecedentdryperiod is less than zero, it must be -88
    print("# CHECK - if antecedentdryperiod is less than zero, it must be -88")

    badrows = precip[
        (precip['antecedentdryperiod'] < 0) & (precip['antecedentdryperiod'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "antecedentdryperiod",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If antecedentdryperiod is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if antecedentdryperiod is less than zero, it must be -88
    print("# END CHECK - if antecedentdryperiod is less than zero, it must be -88")

       

    
    # CHECK - eventid must be a positive integer
    print("# CHECK - eventid must be a positive integer")

    badrows = precip[
        (precip['eventid'] <= 0)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": precip,
        "tablename": 'tbl_precipitation',
        "badrows": badrows,
        "badcolumn": "eventid",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "eventid must be a positive integer"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - eventid must be a positive integer
    print("# END CHECK - eventid must be a positive integer")




    # Check the time format of starttime and endtime columns
    print("# Check the time format of starttime and endtime columns")
    starttime_format_errors = check_time_format(precip, 'starttime')
    endtime_format_errors = check_time_format(precip, 'endtime')

    if not starttime_format_errors and not endtime_format_errors:

        print("No errors found with start/endtime format")

        # CHECK - The start date and time must be before the end date and time (precipitation tab)
        print("# CHECK - The start date and time must be before the end date and time (precipitation tab)")

        # create temporary columns to check if startdatetime is before enddatetime
        precip['startdatetime'] = pd.to_datetime(precip['startdate'].astype(str) + ' ' + precip['starttime'].astype(str), errors='coerce')
        precip['enddatetime'] = pd.to_datetime(precip['enddate'].astype(str) + ' ' + precip['endtime'].astype(str), errors='coerce')
        
        args.update({
            "dataframe": precip,
            "tablename": 'tbl_precipitation',
            "badrows": precip[precip['startdatetime'] >= precip['enddatetime']].tmp_row.tolist(),
            "badcolumn": "startdate,starttime,enddate,endtime",
            "error_type": "Value Error",
            "is_core_error": False,
            "error_message": "The start date and time must be before the end date and time"
        })
        errs = [*errs, checkData(**args)]

        # drop temporary columns
        precip.drop(columns=['startdatetime', 'enddatetime'], inplace=True)

    else:

        print("Errors found with start/endtime format")

        if starttime_format_errors:
            args.update({
                "dataframe": precip,
                "tablename": 'tbl_precipitation',
                "badrows": starttime_format_errors,
                "badcolumn": "starttime",
                "error_type": "Format Error",
                "is_core_error": False,
                "error_message": "Invalid time format in starttime column (should be HH:MM:SS)"
            })
            errs = [*errs, checkData(**args)]
        
        if endtime_format_errors:
            args.update({
                "dataframe": precip,
                "tablename": 'tbl_precipitation',
                "badrows": endtime_format_errors,
                "badcolumn": "endtime",
                "error_type": "Format Error",
                "is_core_error": False,
                "error_message": "Invalid time format in endtime column (should be HH:MM:SS)"
            })
            errs = [*errs, checkData(**args)]

    # END CHECK - The start date and time must be before the end date and time (precipitation tab)
    print("# END CHECK - The start date and time must be before the end date and time (precipitation tab)")




    ######################################################################################################################################
    # ----------------------                         END Precipitation Checks                                    ----------------------- #
    ######################################################################################################################################
    print("# ----------------------                         END Precipitation Checks                                    ----------------------- #")







    print("# ----------------------                         Flow Checks                                    ----------------------- #")
    ######################################################################################################################################
    # ----------------------                         Flow Checks                                    --------------------------- #
    ######################################################################################################################################


    
    # CHECK - if volumetotal is less than zero, it must be -88
    print("# CHECK - if volumetotal is less than zero, it must be -88")

    badrows = flow[
        (flow['volumetotal'] < 0) & (flow['volumetotal'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "volumetotal",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If volumetotal is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if volumetotal is less than zero, it must be -88
    print("# END CHECK - if volumetotal is less than zero, it must be -88")
    



    # CHECK - if peakflowrate is less than zero, it must be -88
    print("# CHECK - if peakflowrate is less than zero, it must be -88")

    badrows = flow[
        (flow['peakflowrate'] < 0) & (flow['peakflowrate'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "peakflowrate",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If peakflowrate is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if peakflowrate is less than zero, it must be -88
    print("# END CHECK - if peakflowrate is less than zero, it must be -88")



       

    
    # CHECK - if hydrographcapturedpct is less than zero, it must be -88
    print("# CHECK - if hydrographcapturedpct is less than zero, it must be -88")

    badrows = flow[
        (flow['hydrographcapturedpct'] < 0) & (flow['hydrographcapturedpct'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": flow,
        "tablename": 'tbl_flow',
        "badrows": badrows,
        "badcolumn": "hydrographcapturedpct",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If hydrographcapturedpct is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if hydrographcapturedpct is less than zero, it must be -88
    print("# END CHECK - if hydrographcapturedpct is less than zero, it must be -88")






    # Check the time format of timestart and timeend columns
    print("# Check the time format of timestart and timeend columns")
    timestart_format_errors = check_time_format(flow, 'timestart')
    timeend_format_errors = check_time_format(flow, 'timeend')

    if not timestart_format_errors and not timeend_format_errors:

        print("No errors found with start/endtime format")

        # CHECK - The start date and time must be before the end date and time (flow tab)
        print("# CHECK - The start date and time must be before the end date and time (flow tab)")

        # create temporary columns to check if startdatetime is before enddatetime
        flow['startdatetime'] = pd.to_datetime(flow['datestart'].astype(str) + ' ' + flow['timestart'].astype(str), errors='coerce')
        flow['enddatetime'] = pd.to_datetime(flow['dateend'].astype(str) + ' ' + flow['timeend'].astype(str), errors='coerce')
        
        args.update({
            "dataframe": flow,
            "tablename": 'tbl_flow',
            "badrows": flow[flow['startdatetime'] >= flow['enddatetime']].tmp_row.tolist(),
            "badcolumn": "datestart,timestart,dateend,timeend",
            "error_type": "Value Error",
            "is_core_error": False,
            "error_message": "The start date and time must be before the end date and time"
        })
        errs = [*errs, checkData(**args)]

        # drop temporary columns
        flow.drop(columns=['startdatetime', 'enddatetime'], inplace=True)

    else:
        
        print("Errors found with start/endtime format")

        if timestart_format_errors:
            args.update({
                "dataframe": flow,
                "tablename": 'tbl_flow',
                "badrows": timestart_format_errors,
                "badcolumn": "timestart",
                "error_type": "Format Error",
                "is_core_error": False,
                "error_message": "Invalid time format in timestart column (should be HH:MM:SS)"
            })
            errs = [*errs, checkData(**args)]
        
        if timeend_format_errors:
            args.update({
                "dataframe": flow,
                "tablename": 'tbl_flow',
                "badrows": timeend_format_errors,
                "badcolumn": "timeend",
                "error_type": "Format Error",
                "is_core_error": False,
                "error_message": "Invalid time format in timeend column (should be HH:MM:SS)"
            })
            errs = [*errs, checkData(**args)]

    # END CHECK - The start date and time must be before the end date and time (flow tab)
    print("# END CHECK - The start date and time must be before the end date and time (flow tab)")




    ######################################################################################################################################
    # ----------------------                         END Flow Checks                                    ----------------------- #
    ######################################################################################################################################
    print("# ----------------------                         END Flow Checks                                    ----------------------- #")


    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #




    ######################################################################################################################################
    # ----------------------                         Water Quality Checks                                    --------------------------- #
    ######################################################################################################################################
    print("# ----------------------                         Water Quality Checks                                    ----------------------- #")


    
    # CHECK - qacode values must be found in the qacode column of the lu_qacode table
    print("# CHECK - qacode values must be found in the qacode column of the lu_qacode table")

    # Query the lu_qacode table to get valid qacode values
    valid_qacodes = pd.read_sql("SELECT qacode FROM lu_qacode", eng)['qacode'].tolist()

    # Find rows in wq where any qacode is not in the valid_qacodes list
    badrows = wq[wq['qacode'].apply(lambda x: any(code.strip() not in valid_qacodes for code in str(x).split(',')))].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "qacode",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Invalid qacode value. It must be found in the lu_qacode table"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - qacode values must be found in the qacode column of the lu_qacode table
    print("# END CHECK - qacode values must be found in the qacode column of the lu_qacode table")





    # CHECK - if result < 0 it must be -88
    print("# CHECK - if result < 0 it must be -88")

    badrows = wq[
        (wq['result'] < 0) & (wq['result'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "result",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If result is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if result < 0 it must be -88
    print("# END CHECK - if result < 0 it must be -88")




    # CHECK - if rl < 0 it must be -88
    print("# CHECK - if rl < 0 it must be -88")

    badrows = wq[
        (wq['rl'] < 0) & (wq['rl'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "rl",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If rl is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if rl < 0 it must be -88
    print("# END CHECK - if rl < 0 it must be -88")





    # CHECK - if mdl < 0 it must be -88
    print("# CHECK - if mdl < 0 it must be -88")

    badrows = wq[
        (wq['mdl'] < 0) & (wq['mdl'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "mdl",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If mdl is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if mdl < 0 it must be -88
    print("# END CHECK - if mdl < 0 it must be -88")




    # CHECK - Fractionname must not be "None" unless the analytename is pH
    print('# CHECK - Fractionname must not be "None" unless the analytename is pH')

    badrows = wq[
        (wq['fractionname'] == 'None') & (wq['analytename'] != 'pH')
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "fractionname",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": 'Fractionname must not be "None" unless the analytename is pH'
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - Fractionname must not be "None" unless the analytename is pH
    print('# END CHECK - Fractionname must not be "None" unless the analytename is pH')





    # CHECK - unitname should match the one from the lu_analyteunits table
    print('# CHECK - unitname should match the one from the lu_analyteunits table')

    # Query the database for lu_analyte and lu_analyteunits, joining on analyteclass to get the correct units
    query = """
        SELECT a.analytename, u.units AS correct_units
        FROM lu_analyte a
        JOIN lu_analyteunits u ON a.analyteclass = u.analyteclass
    """
    analyte_units = pd.read_sql(query, eng)

    # Join wq with the resulting dataframe on the analytename column to tack on the correct_units column
    # If the units column is not specified in the database, we dont need to check for it
    wq_with_units = wq.merge(analyte_units, on='analytename', how='inner')

    # The unitname column of the original wq dataframe should match the one from the lu_analyteunits table
    badrows = wq_with_units[
        wq_with_units['unitname'] != wq_with_units['correct_units']
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "unitname",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": 'The unitname column does not match the expected unit from the <a target="_blank" href="scraper?action=help&layer=lu_analyteunits">lu_analyteunits</a> lookup list'
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - unitname should match the one from the lu_analyteunits table
    print('# END CHECK - unitname should match the one from the lu_analyteunits table')






    # CHECK - sampletypecode should be "EMC-Flow Weighted"
    print('# CHECK - sampletypecode should be "EMC-Flow Weighted"')

    badrows = wq[
        wq['sampletypecode'] != 'EMC-Flow Weighted'
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": wq,
        "tablename": 'tbl_waterquality',
        "badrows": badrows,
        "badcolumn": "sampletypecode",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": 'SampletypeCode should be "EMC-Flow Weighted"'
    })
    warnings = [*warnings, checkData(**args)]

    # END CHECK - sampletypecode should be "EMC-Flow Weighted"
    print('# END CHECK - sampletypecode should be "EMC-Flow Weighted"')




    ######################################################################################################################################
    # ----------------------                         END Water Quality Checks                                    ----------------------- #
    ######################################################################################################################################
    print("# ----------------------                         END Water Quality Checks                                    ----------------------- #")



    errs = [e for e in errs if len(e) > 0]
    warnings = [w for w in warnings if len(w) > 0]

    
    return {'errors': errs, 'warnings': warnings}