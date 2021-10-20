from flask import render_template, request, jsonify, current_app, Blueprint, session, g
from werkzeug.utils import secure_filename
from gc import collect
import os, time, json
import pandas as pd

# custom imports, from local files
from .preprocess import clean_data
from .match import match
from .core.core import core
from .core.functions import fetch_meta
from .utils.generic import save_errors, correct_row_offset
from .utils.excel import mark_workbook
from .utils.mail import send_mail
from .utils.exceptions import default_exception_handler
from .custom.fish_visual_map import fish_visual_map
from .custom.bruv_visual_map import bruv_visual_map
from .custom.veg_visual_map import veg_visual_map
from .custom.sav_visual_map import sav_visual_map


upload = Blueprint('upload', __name__)
@upload.route('/upload',methods = ['GET','POST'])
def main():
    
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
            g.eng.execute(
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
        
        if ((sheet not in current_app.tabs_to_ignore) and (not sheet.startswith('lu_')))
    }
    
    assert len(all_dfs) > 0, f"submissionid - {session.get('submissionid')} all_dfs is empty"
    
    for tblname in all_dfs.keys():
        all_dfs[tblname].columns = [x.lower() for x in all_dfs[tblname].columns]

    print("DONE - building 'all_dfs' dictionary")
    


    # -------------------------------------------------------------------------- #

    # Match tables and dataset routine


    # alter the all_dfs variable with the match function
    # keys of all_dfs should be no longer the original sheet names but rather the table names that got matched, if any
    # if the tab didnt match any table it will not alter that item in the all_dfs dictionary
    print("Running match tables routine")
    match_dataset, match_report, all_dfs = match(all_dfs)
    
    print("match(all_dfs)")
    #print(match(all_dfs))

    print("match_dataset")
    print(match_dataset)
    print("match_report")
    print(match_report)
    print("all_dfs")
    print(all_dfs)

    #NOTE if all tabs in all_dfs matched a database table, but there is still no match_dataset
    # then the problem probably lies in __init__.py

    # need an assert statement
    # an assert statement makes sense because in this would be an issue on our side rather than the user's



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
    g.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET datatype = '{match_dataset}'
        WHERE submissionid = {session.get('submissionid')};
        """
    )


    # ----------------------------------------- #
    # Pre processing data before Core checks
    #  We want to limit the manual cleaning of the data that the user has to do
    #  This function will strip whitespace on character fields and fix columns to match lookup lists if they match (case insensitive)

    #print("preprocessing and cleaning data")
    # We are not sure if we want to do this
    # some projects like bight prohibit this
    all_dfs = clean_data(all_dfs)
    print("DONE preprocessing and cleaning data")
    
    # write all_dfs again to the same excel path
    # Later, if the data is clean, the loading routine will use the tab names to load the data to the appropriate tables
    #   There is an assert statement (in load.py) which asserts that the tab names of the excel file match a table in the database
    #   With the way the code is structured, that should always be the case, but the assert statement will let us know if we messed up or need to fix something 
    #   Technically we could write it back with the original tab names, and use the tab_to_table_map in load.py,
    #   But for now, the tab_table_map is mainly used by the javascript in the front end, to display error messages to the user
    writer = pd.ExcelWriter(excel_path)
    for tblname in all_dfs.keys():
        all_dfs[tblname].to_excel(writer, sheet_name = tblname, startrow = current_app.excel_offset, index=False)
    writer.save()
    
    # Yes this is weird but if we write the all_dfs back to the excel file, and read it back in,
    # this ensures 100% that the data is loaded exactly in the same state as it was in when it was checked
    all_dfs = {
        sheet: pd.read_excel(
            excel_path, 
            sheet_name = sheet,
            skiprows = current_app.excel_offset, 
            na_values = ['']
        )
        for sheet in pd.ExcelFile(excel_path).sheet_names
        if ((sheet not in current_app.tabs_to_ignore) and (not sheet.startswith('lu_')))
    }
    print("all_dfs after read back in")
    print(all_dfs)
    
    # ----------------------------------------- #

    # Core Checks

    # initialize errors and warnings
    errs = []
    warnings = []

    print("Core Checks")

    # meta data is needed for the core checks to run, to check precision, length, datatypes, etc
    dbmetadata = {
        tblname: fetch_meta(tblname, g.eng)
        for tblname in set([y for x in current_app.datasets.values() for y in x.get('tables')])
    }

   
    # tack on core errors to errors list
    
    # debug = False will cause corechecks to run with multiprocessing, 
    # but the logs will not show as much useful information
    print("Right before core runs")
    #core_output = core(all_dfs, g.eng, dbmetadata, debug = False) 
    print("Right after core runs")
    #print("all_dfs")
    #print(all_dfs)
    #print("g.eng")
    #print(g.eng)
    #print("dbmetadata")
    #print(dbmetadata)
    core_output = core(all_dfs, g.eng, dbmetadata, debug = True)
    #print("after core_output, debug TRUE")

    errs.extend(core_output['core_errors'])
    warnings.extend(core_output['core_warnings'])

    # clear up some memory space, i never wanted to store the core checks output in memory anyways 
    # other than appending it to the errors/warnings list
    del core_output
    collect()

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
        print("custom_output: ")
        print(custom_output)
        #example
        #map_output = current_app.datasets.get(match_dataset).get('map_function')(all_dfs)

        assert isinstance(custom_output, dict), \
            "custom output is not a dictionary. custom function is not written correctly"
        assert set(custom_output.keys()) == {'errors','warnings'}, \
            "Custom output dictionary should have keys which are only 'errors' and 'warnings'"

        # tack on errors and warnings
        # errs and warnings are lists initialized in the Core Checks section (above)
        errs.extend(custom_output.get('errors'))
        warnings.extend(custom_output.get('warnings'))

        errs = [e for e in errs if len(e) > 0]
        warnings = [w for w in warnings if len(w) > 0]

        print("errs")
        print(errs)
        print("warnings")
        print(warnings)

        print("DONE - Custom Checks")

    # End Custom Checks section    

    # Begin Visual Map Checks:

    # Run only if they passed Core Checks
    if errs == []:
        # There are visual map checks for SAV, BRUV, Fish and Vegetation:

        map_func = current_app.datasets.get(match_dataset).get('map_func')
        if map_func is not None:
            map_output = map_func(all_dfs, current_app.datasets.get(match_dataset).get('spatialtable'))
            f = open(os.path.join(session.get('submission_dir'),f'{match_dataset}_map.html'),'w')
            f.write(map_output._repr_html_())
            f.close()

    # ---------------------------------------------------------------- #


    # Save the warnings and errors in the current submission directory
    # It would be convenient to store in the session cookie but that has a 4kb limit
    # instead we can just dump it to a json file
    save_errors(warnings, os.path.join( session['submission_dir'], "warnings.json" ))
    save_errors(errs, os.path.join( session['submission_dir'], "errors.json" ))
    print("ASDF")
    # Later we will need to have a way to map the dataframe column names to the column indices
    # This is one of those lines of code where i dont know why it is here, but i have a feeling it will
    #   break things if i delete it
    # Even though i'm the one that put it here... -Robert
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
    print(session.get('excel_path'))

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
        "all_datasets": list(current_app.datasets.keys()),
        "table_to_tab_map" : session['table_to_tab_map']
    }
    
    print(returnvals)

    print("DONE with upload routine, returning JSON to browser")
    return jsonify(**returnvals)


@upload.route('/map/<submissionid>/<datatype>')
def getmap(submissionid, datatype):
    datatype = str(datatype)
    if datatype not in ('sav','bruv','fishseines','vegetation'):
        return "Map not found ¯\_(ツ)_/¯"

    map_path = os.path.join(os.getcwd(), "files", str(submissionid), f'{datatype}_map.html')
    if os.path.exists(map_path):
        html = open(map_path,'r').read()
        return render_template(f'map_template.html', map=html)
    else:
        return "Map not found ¯\_(ツ)_/¯"



# When an exception happens when the browser is sending requests to the upload blueprint, this routine runs
@upload.errorhandler(Exception)
def upload_error_handler(error):
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