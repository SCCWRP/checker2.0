from flask import Blueprint, current_app, session, jsonify, g
from .utils.db import GeoDBDataFrame, next_objectid, registration_id
from .utils.mail import data_receipt
from .utils.exceptions import default_exception_handler
from .core.functions import fetch_meta

import subprocess as sp

from psycopg2.errors import ForeignKeyViolation

import pandas as pd
import json, os

finalsubmit = Blueprint('finalsubmit', __name__)
@finalsubmit.route('/load', methods = ['GET','POST'])
def load():

    # This was put in because there was a bug on the JS side where the form was submitting twice, causing data to attempt to load twice, causing a critical error
    print("REQUEST MADE TO /load")

    assert session.get('submissionid') is not None, "No submissionID, session may have expired"

    # Errors and warnings are stored in a directory in a json since it is likely that they will exceed 4kb in many cases
    # For this reason i didnt use the session cookie
    # Also couldnt figure out how to correctly set up a filesystem session.
    if not pd.DataFrame( json.loads(open(os.path.join(session['submission_dir'], 'errors.json') , 'r').read()) ).empty:
        return jsonify(user_error_message='An attempt was made to do a final submit, but there are errors in the submission')


    # This ensures that the exact file that was originally read in and checked, is being read in the same exact way before the final submit
    # This way, there are not issues with pandas changing data before rewriting the file to excel
    excel_path = session['excel_path']

    eng = g.eng
    
    all_dfs = dict()

    for sheet in pd.ExcelFile(excel_path).sheet_names:
        if ((sheet not in current_app.tabs_to_ignore) and (not sheet.startswith('lu_'))):
            
            converters = fetch_meta(sheet, g.eng, return_converters = True)
            string_converters = converters.get("string_converters")
            timestamp_converters = converters.get("timestamp_converters")
            
            assert string_converters is not None, f"String converters not returned for {sheet} in fetch_meta function"
            assert timestamp_converters is not None, f"Timestamp converters not returned for {sheet} in fetch_meta function"


            # This should never raise an exception - at least converting the columns to str datatypes should never cause a problem
            tmpdf = pd.read_excel(
                excel_path, 
                sheet_name = sheet,
                keep_default_na=False,
                skiprows = current_app.excel_offset,
                na_values = [''],
                converters = string_converters
            )

            # Converting to timestamps may cause a critical error if the user enters a non-valid timestamp literal
            # We do not want to catch the exception here anymore, because if it fails to convert to timestamp, its not going to load to the database anyways
            # This way we will be alerted with a hopefully cleaner error message than the one coming from sqlalchemy
            valid_timestamp_converters = {col: 'datetime64[ns]' for col in timestamp_converters.keys() if col in tmpdf.columns}

            # Now use the filtered dictionary to safely convert types
            tmpdf = tmpdf.astype(valid_timestamp_converters)

            all_dfs[sheet] = tmpdf


    valid_tables = pd.read_sql(
            # percent signs are escaped by doubling them, not with a backslash
            # percent signs need to be escaped because otherwise the python interpreter will think you are trying to create a format string
            f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE (table_name LIKE 'tbl_%%') OR (table_name LIKE 'analysis_%%') OR (table_name = '{current_app.config.get("TOXSUMMARY_TABLENAME")}') """, eng
        ) \
        .table_name \
        .values
    
    print('all_dfs.keys()')
    print(all_dfs.keys())
    print('valid_tables')
    print(valid_tables)
    assert all(sheet in valid_tables for sheet in all_dfs.keys()), \
        f"Sheetname in excel file {excel_path} not found in the list of tables that can be submitted to"

    # read in warnings and merge it to tack on the warnings column
    # only if warnings is non empty
    warnings = pd.DataFrame( json.loads(open(os.path.join(session['submission_dir'], 'warnings.json') , 'r').read()) )
      
    for tbl in all_dfs.keys():

        # Lowercase all column names first
        all_dfs[tbl].columns = [x.lower() for x in all_dfs[tbl].columns]

        #assert not all_dfs[tbl].empty, "Somehow an empty dataframe was about to be submitted"


        if not warnings.empty:
            # warnings
            if not warnings[warnings['table'] == tbl].empty:

                # This is the one liner that tacks on the warnings column
                all_dfs[tbl] = all_dfs[tbl] \
                    .reset_index() \
                    .rename(columns = {'index' : 'row_number'}) \
                    .merge(
                        warnings[warnings['table'] == tbl].rename(columns = {'message':'warnings'})[['row_number','warnings']], 
                        on = 'row_number', 
                        how = 'left'
                    ) \
                    .drop('row_number', axis = 1)
            else:
                all_dfs[tbl] = all_dfs[tbl].assign(warnings = '')
        else:
            all_dfs[tbl] = all_dfs[tbl].assign(warnings = '')
            
        print(session.get('login_info'))
        all_dfs[tbl] = all_dfs[tbl].assign(
            objectid = f"sde.next_rowid('sde','{tbl}')",
            globalid = "sde.next_globalid()",
            created_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            created_user = 'checker',
            last_edited_date = pd.Timestamp(int(session['submissionid']), unit = 's'),
            last_edited_user = 'checker',
            submissionid = session['submissionid']
        ).assign(
            # This will assign all the other necessary columns from the user's login, which typically are appended as columns
            # for BMP specifically we dont want to include the login_testsite
            **{k:v for k,v in session.get('login_info').items() if k != 'login_testsite'}
        )

        all_dfs[tbl] = GeoDBDataFrame(all_dfs[tbl])



    # We have to make an exception for toxsummary, since the summary table gets added after the fact
    analysis_tables = current_app.datasets.get(session.get('datatype')).get('analysis_tables')

    if analysis_tables is None: # not all datatypes have analysis tables
        analysis_tables = []

    assert set(current_app.datasets.get(session.get('datatype')).get('tables')) == set(all_dfs.keys() - set(analysis_tables)), \
            f"""There is a mismatch between the table names listed in __init__.py current_app.datasets
            and the keys of all_dfs (datatype: {session.get('datatype')}"""
    

    # Now go through each tab and load to the database
    tables_to_load = list(
        set(
            [
                *current_app.datasets.get(session.get('datatype')).get('tables'), 
                *analysis_tables
            ]
        )
        .intersection(set(all_dfs.keys()))
    )

    for tbl in tables_to_load:

        # Below comment applied to one project where the tables had foreign key relationships
        # We may or may not want to also apply that to bight
        # print(f"Loading Data to {tbl}. Be sure that the tables are in the correct order in __init__.py datasets")
        print("If foreign key relationships are set, the tables need to be loadede in a particular order")
        
        # These columns are needed in all submission tables, but they are often overlooked
        g.eng.execute(
            f"""
            ALTER TABLE "{tbl}" ADD COLUMN IF NOT EXISTS submissionid int4;
            ALTER TABLE "{tbl}" ADD COLUMN IF NOT EXISTS warnings VARCHAR(5000);
            ALTER TABLE "{tbl}" ADD COLUMN IF NOT EXISTS login_email VARCHAR(50);
            ALTER TABLE "{tbl}" ADD COLUMN IF NOT EXISTS login_agency VARCHAR(50);
            """
        )

        g.eng.execute(
            f"""
            ALTER TABLE {tbl} ALTER COLUMN globalid SET DEFAULT next_globalid();
            ALTER TABLE {tbl} ALTER COLUMN objectid SET DEFAULT next_rowid('sde','{tbl}');
            """
        )

        all_dfs[tbl].to_geodb(tbl, g.eng)

        print(f"done loading data to {tbl}")

        g.eng.execute(
            f"""
            INSERT INTO submission_tracking_checksum
            (objectid, submissionid, tablename, checksum, excel_rows)
            VALUES
            (
                sde.next_rowid('sde','submission_tracking_checksum'),
                {session.get('submissionid')},
                '{tbl}',
                {
                    pd.read_sql(
                        'SELECT COUNT(*) as n_rows FROM {} WHERE submissionid = {}'
                        .format(tbl, session.get('submissionid')),
                        eng
                    )
                    .n_rows 
                    .values[0]
                },
                {len(all_dfs[tbl])}
            )
            ;"""
        )
    
    # So we know the massive argument list of the data receipt function, which is like the notification email for successful submission
    #def data_receipt(send_from, always_send_to, login_email, dtype, submissionid, originalfile, tables, eng, mailserver, *args, **kwargs):
    send_to = current_app.maintainers
    notify = current_app.datasets.get(session.get('datatype')).get("notify")
    if notify is not None:
        send_to = [*send_to, *notify]
    data_receipt(
        send_from = current_app.mail_from,
        always_send_to = send_to,
        login_email = session.get('login_info').get('login_email'),
        dtype = session.get('datatype'),
        submissionid = session.get('submissionid'),
        originalfile = session.get('excel_path'),
        tables = all_dfs.keys(),
        eng = g.eng,
        mailserver = current_app.config['MAIL_SERVER'],
        login_info = session.get('login_info')
    )
    
    # TODO Need to move submitted and marked files to a separate directory that stores submitted files
    # I am having a conversation with myself, but present me, disagrees with past me.
    # The idea behind this is to move them and wipe out the files directory
    # but if stuff has to get deleted anyways, just delete only the ones that show up in this query
    # SELECT submissionid FROM submission_tracking_table WHERE submit != 'yes'


    # They are finally done!
    # Set the submission tracking table record to 'submit = yes'
    # Put their original filename in the submission tracking table
    g.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET submit = 'yes' 
        WHERE submissionid = {session.get('submissionid')};
        """
    )

    # They should not be able to submit with the same SubmissionID
    # Clear session after successful final submit
    session.clear()

    return jsonify(user_notification="Sucessfully loaded data")


# When an exception happens when the browser is sending requests to the finalsubmit blueprint, this routine runs
@finalsubmit.errorhandler(Exception)
def finalsubmit_error_handler(error):
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