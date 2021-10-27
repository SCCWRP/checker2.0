# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
import pandas as pd
from datetime import date
from .functions import checkData, get_badrows

def edna(all_dfs):
    
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
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # These are the dataframes that got submitted for edna

    ednased = all_dfs['tbl_edna_sed_labbatch_data']
    ednawater = all_dfs['tbl_edna_water_labbatch_data']
    ednameta = all_dfs['tbl_edna_metadata']
    ednadata= all_dfs['tbl_edna_data']

    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename":'',
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

    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased[ednased.preparationdate.apply(pd.Timestamp) > ednased.samplecollectiondate.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "preparationdate,samplecollectiondate",
        "error_type" : "Value out of range",
        "error_message" : "Your Collection date should be before your preparation date."
    })
    errs = [*warnings, checkData(**args)]
    

    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased['samplecollectiontime'] == ednased['samplecollectiontime'].apply(lambda x: x.strftime("%I:%M:%S %p") if str(x) != 'nan' else str(x)).index.tolist(),
        "badcolumn": "samplecollectiontime",
        "error_type" : "Time format error",
        "error_message" : "Your collection time format should be 12 HR AM/PM."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased['preparationtime'] == ednased['preparationtime'].apply(lambda x: x.strftime("%I:%M:%S %p") if str(x) != 'nan' else str(x)).index.tolist(),
        "badcolumn": "preparationtime",
        "error_type" : "Time format error",
        "error_message" : "Your preparation time format should be 12 HR AM/PM."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": ednased,
        "tablename": "tbl_edna_sed_labbatch_data",
        "badrows": ednased[ednased.preparationtime.apply(pd.Timestamp) > ednased.samplecollectiontime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "preparationtime, collectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation time should be before collection time."
    })
    errs = [*errs, checkData(**args)]

    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater['samplecollectiontime'] == ednawater['samplecollectiontime'].apply(lambda x: x.strftime("%I:%M:%S %p") if str(x) != 'nan' else str(x)).index.tolist(),
        "badcolumn": "samplecollectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your collection time format should be 12 HR AM/PM"
    })
    errs = [*errs,checkData(**args)]

    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater['preparationtime'] == ednawater['preparationtime'].apply(lambda x: x.strftime("%I:%M:%S %p") if str(x) != 'nan' else str(x)).index.tolist(),
        "badcolumn": "preparationtime",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation time format should be 12 HR AM/PM"
    })
    errs = [*errs,checkData(**args)]

    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater[ednawater.preparationtime.apply(pd.Timestamp) > ednawater.samplecollectiontime.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "preparationtime,samplecollectiontime",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation time should be before your collection time"
    })
    errs = [*errs,checkData(**args)]

    args.update({
        "dataframe": ednawater,
        "tablename": "tbl_edna_water_labbatch_data",
        "badrows": ednawater[ednawater.preparationdate.apply(pd.Timestamp) > ednawater.samplecollectiondate.apply(pd.Timestamp)].index.tolist(),
        "badcolumn": "preparationdate,samplecollectiondate",
        "error_type" : "Value out of range",
        "error_message" : "Your preparation date should be before your collection date."
    })
    errs = [*warnings, checkData(**args)]
    
    return {'errors': errs, 'warnings': warnings}