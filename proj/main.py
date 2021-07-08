from flask import render_template, request, jsonify, current_app, Blueprint, session
from werkzeug.utils import secure_filename
import os, time, json
import pandas as pd

# custom imports, from local files
from .match import match
from .core.core import core
from .core.functions import fetch_meta
from .utils.generic import save_errors

homepage = Blueprint('homepage', __name__)
@homepage.route('/')
def index():
    if not session.get('submissionid'):
        session['submissionid'] = int(time.time())
        session['submission_dir'] = os.path.join(os.getcwd(), "files", str(session['submissionid']))
        os.mkdir(session['submission_dir'])


    return render_template('index.html')



@homepage.route('/login', methods = ['GET','POST'])
def login():

    login_info = dict(request.form)

    for k,v in login_info.items():    
        session[k] = v

    return jsonify(msg="login successful")


    
@homepage.route('/upload',methods = ['GET','POST'])
def upload():
    
    # -------------------------------------------------------------------------- #

    # First, the routine to upload the file(s)

    # routine to grab the uploaded file
    print("uploading files")
    files = request.files.getlist('files[]')
    if len(files) > 0:
        
        # TODO Need logic to ensure that there is only one excel file
        
        for f in files:
            # i'd like to figure a way we can do it without writing the thing to an excel file
            f = files[0]
            filename = secure_filename(f.filename)

            # if file extension is xlsx/xls (hopefully xlsx)
            excel_path = os.path.join( session['submission_dir'], str(filename) )

            # the user's uploaded excel file can now be read into pandas
            f.save(excel_path)

            # To be accessed later by the upload routine that loads data to the tables
            session['excel_path'] = excel_path

    else:
        return jsonify(msg="No file given")

    print("DONE uploading files")


    # -------------------------------------------------------------------------- #
    
    # Read in the excel file to make a dictionary of dataframes (all_dfs)

    assert isinstance(current_app.excel_offset, int), "Number of rows to offset in excel file must be an integer. Check__init__.py"

    # build all_dfs where we will store their data
    print("building 'all_dfs' dictionary")
    all_dfs = {

        # Some projects may have descriptions in the first row, which are not the column headers
        # This is the only reason why the skiprows argument is used.
        # For projects that do not set up their data templates in this way, that arg should be removed

        # Note also that only empty cells will be regarded as missing values
        sheet: pd.read_excel(
            excel_path, 
            sheet_name = sheet, 
            skiprows = current_app.excel_offset, 
            na_values = ['']
        )
        
        for sheet in pd.ExcelFile(excel_path).sheet_names
        
        if sheet not in current_app.tabs_to_ignore
    }
    print("DONE - building 'all_dfs' dictionary")


    # -------------------------------------------------------------------------- #

    # Match tables and dataset routine


    # alter the all_dfs variable with the match function
    # keys of all_dfs should be no longer the original sheet names but rather the table names that got matched, if any
    # if the tab didnt match any table it will not alter that item in the all_dfs dictionary
    print("Running match tables routine")
    match_dataset, match_report, all_dfs = match(all_dfs)
    print("DONE - Running match tables routine")


    if any([x['tablename'] == "" for x in match_report]):
        # A tab in their excel file did not get matched with a table
        # return to user
        print("A tab in their excel file was not matched to any table in the database. Returning JSON response to browser")
        return jsonify(
            filename = filename,
            match_report = match_report,
            match_dataset = match_dataset,
            matched_all_tables = False
        )


    # ----------------------------------------- #

    # Core Checks

    # initialize errors and warnings
    errs = []
    warnings = []

    print("Core Checks")

    # meta data is needed for the core checks to run, to check precision, length, datatypes, etc
    dbmetadata = {
        tblname: fetch_meta(tblname, current_app.eng)
        for tblname in set([y for x in current_app.datasets.values() for y in x.get('tables')])
    }

    # tack on core errors to errors list
    errs.extend(
        # debug = False will cause corechecks to run with multiprocessing, 
        # but the logs will not show as much useful information
        core(all_dfs, current_app.eng, dbmetadata, debug = False)
    )
    print("DONE - Core Checks")

    # ----------------------------------------- #

    

    # Custom Checks based on match dataset

    assert match_dataset in current_app.datasets.keys(), \
        f"match dataset {match_dataset} not found in the current_app.datasets.keys()"


    # if there are no core errors, run custom checks

    # Users complain about this. 
    # However, often times, custom check functions make basic assumptions about the data,
    # which would depend on the data passing core checks. 
    
    # For example, it may assume that a certain column contains only numeric values, in order to check if the number
    # falls within an expected range of values, etc.
    # This makes the assumption that all values in that column are numeric, which is checked and enforced by Core Checks

    if errs == []: 
        print("Custom Checks")
        print(f"Datatype: {match_dataset}")

        # custom output should be a dictionary where errors and warnings are the keys and the values are a list of "errors" 
        # (structured the same way as errors are as seen in core checks section)

        # The custom checks function is stored in __init__.py in the datasets dictionary and accessed and called accordingly
        custom_output = current_app.datasets.get(match_dataset).get('function')(all_dfs)

        assert isinstance(custom_output, dict), \
            "custom output is not a dictionary. custom function is not written correctly"
        assert set(custom_output.keys()) == {'errors','warnings'}, \
            "Custom output dictionary should have keys which are only 'errors' and 'warnings'"

        # tack on errors and warnings
        # errs and warnings are lists initialized in the Core Checks section (above)
        errs.extend(custom_output.get('errors'))
        warnings.extend(custom_output.get('warnings'))

        print("DONE - Custom Checks")

    # End Custom Checks section    


    # ---------------------------------------------------------------- #


    # Save the warnings and errors in the current submission directory
    # It would be convenient to store in the session cookie but that has a 4kb limit
    # instead we can just dump it to a json file
    save_errors(warnings, os.path.join( session['submission_dir'], "warnings.json" ))
    save_errors(errs, os.path.join( session['submission_dir'], "errors.json" ))
    
    # Later we will need to have a way to map the dataframe column names to the column indices
    session['col_indices'] = {tbl: {col: df.columns.get_loc(col) for col in df.columns} for tbl, df in all_dfs.items() }
    

    # ---------------------------------------------------------------- #

    # By default the error and warnings collection methods assume that no rows were skipped in reading in of excel file.
    # It adds 1 to the row number when getting the error/warning, since excel is 1 based but the python dataframe indexing is zero based.
    # Therefore the row number in the errors and warnings will only match with their excel file's row if the column headers are actually in 
    #   the first row of the excel file.
    # These next few lines of code should correct that

    for lst in (errs, warnings):
        [
            r.update(
                {
                    'message'     : r['message'],
                    
                    # to get the actual excel file row number, we must add the number of rows that pandas skipped while first reading in the dataframe,
                    #   and we must add another row to account for the row in the excel file that contains the column headers 
                    #   and another 1 to account for the 1 based indexing of excel vs the zero based indexing of python
                    'row_number'  : r['row_number'] + current_app.excel_offset + 1 + 1 ,
                    'value'       : r['value']
                }
            )
            for e in lst
            for r in e['rows']
        ]


    # ---------------------------------------------------------------- #


    # These are the values we are returning to the browser as a json
    # https://pics.me.me/code-comments-be-like-68542608.png
    returnvals = {
        "filename" : filename,
        "match_report" : match_report,
        "matched_all_tables" : True,
        "match_dataset" : match_dataset,
        "errs" : errs,
        "warnings": warnings
    }
    
    print("DONE with upload routine, returning JSON to browser")
    return jsonify(**returnvals)


@homepage.route('/reset', methods = ['GET','POST'])
def clearsession():
    session.clear()
    return jsonify(msg="session cleared")