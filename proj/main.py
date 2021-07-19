from flask import render_template, request, jsonify, current_app, Blueprint, session
from werkzeug.utils import secure_filename
import os, time, json
import pandas as pd

# custom imports, from local files
from .match import match
from .core.core import core
from .core.functions import fetch_meta
from .utils.generic import save_errors, correct_row_offset
from .utils.excel import mark_workbook
from .utils.mail import send_mail
from .utils.exceptions import default_exception_handler


homepage = Blueprint('homepage', __name__)
@homepage.route('/')
def index():
    eng = current_app.eng

    # upon new request clear session, reset submission ID, reset submission directory
    session.clear()

    session['submissionid'] = int(time.time())
    session['submission_dir'] = os.path.join(os.getcwd(), "files", str(session['submissionid']))
    os.mkdir(session['submission_dir'])

    assert \
        len(
            pd.read_sql(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_name IN ('submission_tracking_table','submission_tracking_checksum')
                """,
                eng
            )
        ) == 2, \
        "Database is missing submission_tracking_table and/or submission_tracking_checksum"


    # insert a record into the submission tracking table
    eng.execute(
        f"""
        INSERT INTO submission_tracking_table
        (objectid, submissionid, created_date, last_edited_date, last_edited_user) 
        VALUES (
            sde.next_rowid('sde','submission_tracking_table'), 
            {session.get('submissionid')},
            '{pd.Timestamp(session.get('submissionid'), unit = 's')}',
            '{pd.Timestamp(session.get('submissionid'), unit = 's')}',
            'checker'
        );
        """
    )
    return render_template('index.html', projectname=current_app.project_name)



@homepage.route('/login', methods = ['GET','POST'])
def login():

    login_info = dict(request.form)
    print(login_info)
    session['login_info'] = login_info

    # The info from the login form needs to be in the system fields list, otherwise it will throw off the match routine
    assert set(login_info.keys()).issubset(set(current_app.system_fields)), \
        f"{','.join(set(login_info.keys()) - set(current_app.system_fields))} not found in the system fields list"

    assert "login_email" in login_info.keys(), \
        "No email address found in login form. It should be named login_email since the email notification routine assumes so."

    assert all([str(x).startswith('login_') for x in login_info.keys()]), \
        "The login form failed for follow the naming convention of having all input names begin with 'login_'"

    # Update submission tracking, putting their email address in their record
    current_app.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET login_email = '{login_info.get('login_email')}' 
        WHERE submissionid = {session.get('submissionid')};
        """
    )

    return jsonify(msg="login successful")


    
@homepage.route('/upload',methods = ['GET','POST'])
def upload():
    
    # -------------------------------------------------------------------------- #

    # First, the routine to upload the file(s)

    # routine to grab the uploaded file
    print("uploading files")
    files = request.files.getlist('files[]')
    if len(files) > 0:
        
        if sum(['xls' in secure_filename(x.filename).rsplit('.',1)[-1] for x in files]) > 1:
            return jsonify(user_error_msg='You have submitted more than one excel file')
        
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

            # Put their original filename in the submission tracking table
            current_app.eng.execute(
                f"""
                UPDATE submission_tracking_table 
                SET original_filename = '{filename}' 
                WHERE submissionid = {session.get('submissionid')};
                """
            )

    else:
        return jsonify(user_error_msg="No file given")

    # We are assuming filename is an excel file
    if '.xls' not in filename:
        errmsg = f"filename: {filename} appears to not be what we would expect of an excel file.\n"
        errmsg += "As of right now, the application can accept one excel file at a time.\n"
        errmsg += "If you are submitting data for multiple tables, they should be separate tabs in the excel file."
        return jsonify(user_error_msg=errmsg)

    print("DONE uploading files")

    # -------------------------------------------------------------------------- #
    
    # Read in the excel file to make a dictionary of dataframes (all_dfs)

    assert isinstance(current_app.excel_offset, int), \
        "Number of rows to offset in excel file must be an integer. Check__init__.py"

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

    for tblname in all_dfs.keys():
        all_dfs[tblname].columns = [x.lower() for x in all_dfs[tblname].columns]

    print("DONE - building 'all_dfs' dictionary")


    # -------------------------------------------------------------------------- #

    # Match tables and dataset routine


    # alter the all_dfs variable with the match function
    # keys of all_dfs should be no longer the original sheet names but rather the table names that got matched, if any
    # if the tab didnt match any table it will not alter that item in the all_dfs dictionary
    print("Running match tables routine")
    match_dataset, match_report, all_dfs = match(all_dfs, current_app.eng, current_app.system_fields, current_app.datasets)
    print("DONE - Running match tables routine")

    session['datatype'] = match_dataset

    print("match_dataset")
    print(match_dataset)

    if match_dataset == "":
        # A tab in their excel file did not get matched with a table
        # return to user
        print("Failed to match a dataset")
        return jsonify(
            filename = filename,
            match_report = match_report,
            match_dataset = match_dataset,
            matched_all_tables = False
        )
    
    # If they made it this far, a dataset was matched
    current_app.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET datatype = '{match_dataset}' 
        WHERE submissionid = {session.get('submissionid')};
        """
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
        #core(all_dfs, current_app.eng, dbmetadata, debug = True)
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

    errs = correct_row_offset(errs, offset = current_app.excel_offset)
    warnings = correct_row_offset(warnings, offset = current_app.excel_offset)


    # -------------------------------------------------------------------------------- #

    # Mark up the excel workbook
    print("Marking Excel file")

    # mark_workbook function returns the file path to which it saved the marked excel file
    session['marked_excel_path'] = mark_workbook(
        all_dfs = all_dfs, 
        excel_path = session.get('excel_path'), 
        errs = errs, 
        warnings = warnings
    )

    print("DONE - Marking Excel file")

    # -------------------------------------------------------------------------------- #


    # These are the values we are returning to the browser as a json
    # https://pics.me.me/code-comments-be-like-68542608.png
    returnvals = {
        "filename" : filename,
        "marked_filename" : f"{filename.rsplit('.',1)[0]}-marked.{filename.rsplit('.',1)[-1]}",
        "match_report" : match_report,
        "matched_all_tables" : True,
        "match_dataset" : match_dataset,
        "errs" : errs,
        "warnings": warnings,
        "submissionid": session.get("submissionid"),
        "critical_error": False,
        "all_datasets": list(current_app.datasets.keys())
    }
    
    print(returnvals)

    print("DONE with upload routine, returning JSON to browser")
    return jsonify(**returnvals)



# When an exception happens when the browser is sending requests to the homepage blueprint, this routine runs
@homepage.errorhandler(Exception)
def homepage_error_handler(error):
    response = default_exception_handler(
        mail_from = current_app.mail_from,
        errmsg = str(error),
        maintainers = current_app.maintainers,
        project_name = current_app.project_name,
        attachment = session.get('excel_path'),
        login_info = session.get('login_info'),
        submissionid = session.get('submissionid'),
        mail_server = current_app.config['MAIL_SERVER']
    )
    return response