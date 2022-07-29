# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, get_badrows
import pandas as pd

def chemistry(all_dfs):
    
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
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    chem = all_dfs['tbl_chemistryresults']


    args = {
        "dataframe": chem,
        "tablename": 'tbl_chemistryresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    lu_analyte = pd.read_sql("SELECT * FROM lu_analyte", g.eng)

    chem = chem.merge(lu_analyte[['analytename','analyteclass']], on = 'analytename', how = 'left')


    print("Check - For Nutrient analytes, results must be reported in mg/L")
    badrows = chem[(chem.analyteclass.isin(['Nutrients'])) & (~chem.unit.isin(['mg/L']))].index.tolist()
    args.update({
        "badrows": badrows,
        "badcolumn": "unit",
        "error_type" : "Incorrect Units",
        "error_message" : "For Nutrient analytes, results must be reported in mg/L"
    })
    errs = [*errs, checkData(**args)]


    print("Check - For all analytes other than those deemed to be Nutrients, results must be reported in ug/L")
    badrows = chem[(~chem.analyteclass.isin(['Nutrients'])) & (~chem.unit.isin(['ug/L']))].index.tolist()
    args.update({
        "badrows": badrows,
        "badcolumn": "unit",
        "error_type" : "Incorrect Units",
        "error_message" : "For all analytes other than those deemed to be Nutrients, results must be reported in ug/L"
    })
    errs = [*errs, checkData(**args)]

    print("Check - The Result value should not be a negative number")
    badrows = chem[chem.result < 0].index.tolist()
    args.update({
        "badrows": badrows,
        "badcolumn": "result",
        "error_type" : "Incorrect Value",
        "error_message" : "The Result value should not be a negative number - for a Non detect/Not analyzed, simply leave the cell blank and put 'Not Detected' or 'Not Analyzed' in the ResultQualifier column"
    })
    errs = [*errs, checkData(**args)]


    print("Check - If Result value is a non-negative value which is less than the RL, this is considered a Non Detect, so the Result Qualifier should say 'Not Detected'")
    badrows = chem[(chem.result >= 0) & (chem.result < chem.rl) & (~chem.resultqualifier.isin(['Not Detected']))].index.tolist()
    args.update({
        "badrows": badrows,
        "badcolumn": "resultqualifier",
        "error_type" : "Incorrect Value",
        "error_message" : "The result value is a non-negative value which is less than the RL, so this is considered a Non Detect, but the Result Qualifier does not say 'Not Detected'"
    })
    errs = [*errs, checkData(**args)]


    print("Check - If Result value is missing, the result qualifier should be Not Analyzed or Not Detected")
    badrows = chem[pd.isnull(chem.result) & (~chem.resultqualifier.isin(['Not Analyzed','Not Detected']))].index.tolist()
    args.update({
        "badrows": badrows,
        "badcolumn": "result",
        "error_type" : "Incorrect Value",
        "error_message" : "The Result value is missing, therefore the Result Qualifier should be 'Not Analyzed', or 'Not Detected'"
    })
    errs = [*errs, checkData(**args)]





    return {'errors': errs, 'warnings': warnings}
