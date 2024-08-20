# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, mismatch
import pandas as pd
import re


def ptsensor(all_dfs):
    
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

    ptsensorresults = all_dfs['tbl_ptsensorresults']
    ptsensorresults = ptsensorresults.assign(tmp_row = ptsensorresults.index)

    ptsensorresults_args = {
        "dataframe": ptsensorresults,
        "tablename": 'tbl_ptsensorresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    ## LOGIC ##
    print("Starting PTSensor Logic Checks")
    #Jordan - Station occupation and trawl event data should be submitted before pressure temperature data. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number]
    print('Station occupation and trawl event data should be submitted before pressure temperature data. Check those tables to make sure the agency has submitted those first. [records are matched on StationID, SampleDate, Sampling Organization, and Trawl Number] ')

    matchcols = ['stationid','sampledate','samplingorganization','trawlnumber']
    trawlevent = pd.read_sql("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;", g.eng)
    
    ptsensorresults_args.update({
        "badrows": mismatch(
            # got an error about datatypes with the columns - not sure why
            ptsensorresults.assign(sampledate = ptsensorresults.sampledate.astype(str)), 
            trawlevent.assign(sampledate = trawlevent.sampledate.astype(str)), 
            matchcols
        ),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in ptsensorresults must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**ptsensorresults_args)]

    ## END LOGIC CHECKS ##

    ## CUSTOM CHECKS ##
    
    
    # Check - sensortime must be in a 24 hour clock format   
    def checkTime(df, col, args, time_format = re.compile(r'^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$'), custom_errmsg = None):
        """default to checking the 24 hour clock time"""
        args.update({
            "badrows": df[~df[col.lower()].apply(lambda x: bool(time_format.match(str(x).strip())) )].tmp_row.tolist(),
            "badcolumn": col,
            "error_type" : "Formatting Error",
            "error_message" : f"The column {col} is not in a valid 24 hour clock format (HH:MM:SS)" if not custom_errmsg else custom_errmsg
        })
        return checkData(**args)

    errs = [*errs, checkTime(ptsensorresults, 'sensortime', ptsensorresults_args)]


    ## END CUSTOM CHECKS ##


    return {'errors': errs, 'warnings': warnings}
