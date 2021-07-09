from flask import Blueprint, current_app, session, jsonify
from .utils.db import GeoDBDataFrame

import pandas as pd
import json, os

finalsubmit = Blueprint('finalsubmit', __name__)
@finalsubmit.route('/load', methods = ['GET','POST'])
def load():

    # Errors and warnings are stored in a directory in a json since it is likely that they will exceed 4kb in many cases
    # For this reason i didnt use the session cookie
    # Also couldnt figure out how to correctly set up a filesystem session.
    if not pd.DataFrame( json.loads(open(os.path.join(session['submission_dir'], 'errors.json') , 'r').read()) ).empty:
        return jsonify(user_error_message='An attempt was made to do a final submit, but there are errors in the submission')


    excel_path = session['excel_path']

    eng = current_app.eng

    all_dfs = {

        # Some projects may have descriptions in the first row, which are not the column headers
        # This is the only reason why the skiprows argument is used.
        # For projects that do not set up their data templates in this way, that arg should be removed

        # Note also that only empty cells will be regarded as missing values
        sheet: pd.read_excel(excel_path, sheet_name = sheet, skiprows = current_app.excel_offset, na_values = [''])
        
        for sheet in pd.ExcelFile(excel_path).sheet_names
        
        if sheet not in current_app.tabs_to_ignore
    }

    valid_tables = pd.read_sql(
            # percent signs are escaped by doubling them, not with a backslash
            # percent signs need to be escaped because otherwise the python interpreter will think you are trying to create a format string
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl_%%'", eng
        ) \
        .table_name \
        .values

    assert all(sheet in valid_tables for sheet in all_dfs.keys()), \
        f"Sheetname in excel file {excel_path} not found in the list of tables that can be submitted to"

    # read in warnings and merge it to tack on the warnings column
    # only if warnings is non empty
    warnings = pd.DataFrame( json.loads(open(os.path.join(session['submission_dir'], 'warnings.json') , 'r').read()) )
    
    for tbl, df in all_dfs.items():
        assert not df.empty, "Somehow an empty dataframe was about to be submitted"

        # warnings
        if not warnings[warnings['table'] == tbl].empty:

            # This is the one liner that tacks on the warnings column
            all_dfs[tbl] = df \
                .reset_index() \
                .rename(columns = {'index' : 'row_number'}) \
                .merge(
                    warnings[warnings['table'] == tbl].rename(columns = {'message':'warnings'})[['row_number','warnings']], 
                    on = 'row_number', 
                    how = 'left'
                ) \
                .drop('row_number', axis = 1)
        else:
            all_dfs[tbl] = df.assign(warnings = '')
            
        all_dfs[tbl] = df.assign(
            objectid = f"sde.next_rowid('sde','{tbl}')",
            globalid = "sde.next_globalid()",
            created_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            created_user = 'checker',
            last_edited_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            last_edited_user = 'checker',
            submissionid = session['submissionid']
        ).assign(
            # This will assign all the other necessary columns from the user's login, which typically are appended as columns
            **session['login_info']
        )



        all_dfs[tbl] = GeoDBDataFrame(df, current_app.eng)


    for tbl, df in all_dfs.items():
        df.to_geodb(tbl)
        print(f"done loading data to {tbl}")
    return jsonify(user_notification="Sucessfully loaded data")

