from flask import render_template, request, jsonify, current_app, Blueprint, session
from werkzeug.utils import secure_filename
import os, time
import pandas as pd

# custom imports, from local files
from .match import match
from .core.core import core
from .custom.func1 import func1
from .custom.func2 import func2

homepage = Blueprint('homepage', __name__)
@homepage.route('/')
def index():
    if not session.get('submissionid'):
        session['submissionid'] = int(time.time())
        os.mkdir(os.path.join(os.getcwd(), "files", str(session['submissionid'])))

    return render_template('index.html')



@homepage.route('/login', methods = ['GET','POST'])
def login():
    email_login = request.form.get("email")
    agency = request.form.get("agency")

    session['agency'] = agency
    session['email_login'] = email_login

    print(agency, email_login)

    return jsonify(msg="login successful")


    
@homepage.route('/upload',methods = ['GET','POST'])
def upload():
    
    # routine to grab the uploaded file
    files = request.files.getlist('files[]')
    print("files")
    print(files)
    if len(files) > 0:
        
        # TODO Need logic to ensure that there is only one excel file
        
        for f in files:
            # i'd like to figure a way we can do it without writing the thing to an excel file
            f = files[0]
            filename = secure_filename(f.filename)

            # if file extension is xlsx/xls (hopefully xlsx)
            excel_path = f"{os.getcwd()}/files/{session['submissionid']}/{filename}"

            # the user's uploaded excel file can now be read into pandas
            f.save(excel_path)

    else:
        return jsonify(msg="No file given")

    # build all_dfs where we will store their data
    all_dfs = {
        s: pd.read_excel(
            excel_path
            , sheet_name = s 

            # Some projects may have descriptions in the first row, which are not the column headers
            , skiprows=[0]
        )
        
        for s in pd.ExcelFile(excel_path).sheet_names
        
        if s not in current_app.tabs_to_ignore
    }

    print(all_dfs)

    # alter the all_dfs variable with the match function
    # keys of all_dfs should be no longer the original sheet names but rather the table names that got matched, if any
    # if the tab didnt match any table it will not alter that item in the all_dfs dictionary
    match_dataset, match_report, all_dfs = match(all_dfs)

    print(match_report)

    if any([x['tablename'] == "" for x in match_report]):
        # A tab in their excel file did not get matched with a table
        # returrn to user
        return jsonify(
            filename = filename,
            match_report = match_report,
            match_dataset = match_dataset,
            matched_all_tables = False
        )


    # Core Checks
    # debug = False will cause corechecks to run with multiprocessing, but the logs will not show as much useful information

    # initialize errors and warnings
    errs = []
    warnings = []

    # tack on core errors to errors list
    errs.extend(
        core(all_dfs, current_app.eng, current_app.dbmetadata, debug = True)
    )


    
    # Custom Checks based on match dataset
    custom_functions = {
        "test1": func1,
        "test2": func2,
    }

    if match_dataset in custom_functions.keys() and errs == []: # no core errors

        # custom output should be a dictionary where errors and warnings are the keys and the values are a list of "errors" 
        # (structured the same way as errors are as seen in core checks section)
        custom_output = custom_functions.get(match_dataset)(all_dfs)

        # tack on errors and warnings
        errs.extend(custom_output.get('errors'))
        warnings.extend(custom_output.get('warnings'))

    # End Custom Checks section    




    returnvals = {
        "filename" : filename,
        "match_report" : match_report,
        "matched_all_tables" : True,
        "match_dataset" : match_dataset,
        "errs" : errs,
        "warnings": warnings
    }
    return jsonify(**returnvals)


@homepage.route('/reset', methods = ['GET','POST'])
def clearsession():
    session.clear()
    return jsonify(msg="session cleared")