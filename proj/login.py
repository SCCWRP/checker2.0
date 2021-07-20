import time, os
import pandas as pd
from flask import session, Blueprint, current_app, request, render_template, jsonify
from .utils.exceptions import default_exception_handler

homepage = Blueprint('homepage', __name__)
@homepage.route('/', methods = ['GET','POST'])
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
    sitecodes = pd.read_sql(
            "SELECT DISTINCT sitecode FROM unified_main ORDER BY sitecode;", eng
        ) \
        .sitecode \
        .tolist()

    return render_template(
        'index.html', 
        projectname = current_app.project_name,
        sitecodes = sitecodes
    )



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
    # this assumes that the fields are named exactly the same as the login form
    current_app.eng.execute(
        f"""
        UPDATE submission_tracking_table 
        SET {
            ','.join([
                "{} = '{}'".format(k, v)
                for k, v in login_info.items()
            ])
        }
        WHERE submissionid = {session.get('submissionid')};
        """
    )

    return jsonify(msg="login successful")


@homepage.route('/collectiondates', methods = ['GET','POST'])
def collectiondates():
    
    eng = current_app.eng

    sitecode = request.form.get('login_sitecode')

    assert sitecode in pd.read_sql("SELECT DISTINCT sitecode FROM unified_main", eng).sitecode.values, \
        "Bad request to /collectiondates - sitecode not found in unified_main"
    
    collectiondates = pd.read_sql(
            f"""SELECT DISTINCT collectiondate FROM unified_main 
            WHERE sitecode = '{sitecode}' 
            AND collectiondate IS NOT NULL""",
            eng
        ) \
        .collectiondate \
        .tolist()
    
    [pd.Timestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in collectiondates]

    return jsonify(collectiondates=collectiondates)

@homepage.route('/pendantids', methods = ['GET','POST'])
def pendantids():
    
    eng = current_app.eng

    sitecode = request.form.get('login_sitecode')
    collectiondate = request.form.get('login_collectiondate')

    # Javascript sends it in some kind of weird date format, so we have to reformat it
    collectiondate = pd.Timestamp(collectiondate).strftime("%Y-%m-%d %H:%M:%S")

    assert sitecode in pd.read_sql("SELECT DISTINCT sitecode FROM unified_main", eng) \
        .sitecode.values, \
        "Bad request to /pendantids - sitecode not found in unified_main"
    assert collectiondate in pd.read_sql(
            "SELECT DISTINCT collectiondate FROM unified_main WHERE collectiondate IS NOT NULL", 
            eng
        ) \
        .collectiondate \
        .apply(lambda x: pd.Timestamp(x).strftime("%Y-%m-%d %H:%M:%S")) \
        .values, \
        "Bad request to /pendantids - collectiondate not found in unified_main"
    
    pendantids = pd.read_sql(
            # that column name should be pendant id rather than pendent
            f"""
            SELECT DISTINCT l1_pendent_id FROM unified_main 
            WHERE sitecode = '{sitecode}'
            AND collectiondate = '{collectiondate}'
            ;""",
            eng
        ) \
        .l1_pendent_id \
        .tolist()
    
    return jsonify(pendantids=pendantids)


    
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