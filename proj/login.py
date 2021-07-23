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

    # Only return list of sitecodes which have been revisited
    sitecodes = pd.read_sql(
            """
            SELECT
                DISTINCT sitecode
            FROM
                vw_logger_deployment
            """,
            eng                                
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

    # Something that may or may not be specific for this project, but
    #  based on the dataset, there are different login fields that are relevant
    assert login_info.get('login_datatype') in current_app.datasets.keys(), f"login_datatype form field value {login_info.get('login_datatype')} not found in current_app.datasets.keys()"
    session['login_info'] = {k: v for k,v in login_info.items() if k in current_app.datasets.get(login_info.get('login_datatype')).get('login_fields')}
    
    print(session.get('login_info'))
    
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


@homepage.route('/startdates', methods = ['GET','POST'])
def startdates():
    
    eng = current_app.eng

    sitecode = request.form.get('login_sitecode')
    lognum = request.form.get('login_loggernumber')
    pendantid = request.form.get('login_pendantid')

    # prevent sql injection
    assert sitecode in pd.read_sql("SELECT DISTINCT sitecode FROM vw_logger_deployment", eng).sitecode.values, \
        f"Bad request to /startdates - sitecode {sitecode} not found in vw_logger_deployment"
    assert int(lognum) in (1,2), f"Bad request to /startdates - lognum {lognum} not found in vw_logger_deployment"
    assert int(pendantid) in pd.read_sql(f"SELECT l{lognum}_pendent_id::INTEGER AS pendantid FROM vw_logger_deployment", eng) \
        .pendantid \
        .values, \
        f"Bad request to /startdates - L{lognum} pendantid {pendantid} not found in vw_logger_deployment"
    sql = f"""
            SELECT DISTINCT
            collectiondate AS startdate 
        FROM
            vw_logger_deployment
        WHERE
            sitecode = '{sitecode}' 
            AND l{lognum}_pendent_id :: INTEGER = {pendantid} 
            AND collectiondate != (
            SELECT
            CASE
                WHEN
                    ( SELECT 
                        COUNT ( * ) FROM vw_logger_deployment
                        WHERE sitecode = '{sitecode}' AND l{lognum}_pendent_id :: INTEGER = {pendantid} AND collectiondate IS NOT NULL ) = 1 
                THEN
                    '1000-01-01 00:00:00' 
                ELSE MAX ( collectiondate ) 
                END 
            FROM
                vw_logger_deployment 
            WHERE
                sitecode = '{sitecode}' 
            AND l{lognum}_pendent_id :: INTEGER = {pendantid} 
            );
            """
    startdates = pd.read_sql(sql, eng) \
        .startdate \
        .values
    
    startdates = [pd.Timestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in startdates]

    return jsonify(startdates=startdates)


@homepage.route('/enddates', methods = ['GET','POST'])
def enddates():
    
    eng = current_app.eng

    sitecode = request.form.get('login_sitecode')
    startdate = request.form.get('login_start')

    # prevent sql injection
    assert sitecode in pd.read_sql("SELECT DISTINCT sitecode FROM vw_logger_deployment", eng).sitecode.values, \
        "Bad request to /enddates - sitecode not found in vw_logger_deployment"
    try:
        startdate = pd.Timestamp(startdate).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(e)
        raise Exception(f"startdate form value {startdate} unable to be coerced to timestamp.")
    
    enddates = pd.read_sql(
            f"""
            SELECT
                DISTINCT MIN(collectiondate) AS enddate
            FROM
                vw_logger_deployment 
            WHERE  sitecode = '{sitecode}' 
                and collectiondate > '{startdate}'
            """,
            eng
        ) \
        .enddate \
        .values
    
    # We know it will be one value, but i'll return it as an array anyways
    enddates = [pd.Timestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in enddates if not pd.isnull(x)]

    return jsonify(enddates=enddates)

@homepage.route('/pendantids', methods = ['GET','POST'])
def pendantids():
    
    eng = current_app.eng

    sitecode = request.form.get('login_sitecode')
    lognum = request.form.get('login_loggernumber')

    assert sitecode in pd.read_sql("SELECT DISTINCT sitecode FROM unified_main", eng) \
        .sitecode.values, \
        "Bad request to /pendantids - sitecode not found in unified_main"
    assert int(lognum) in (1,2), "Bad request to /pendantids - loggernumber should be 1 or 2"
    
    pendantids = pd.read_sql(
            # that column name should be pendant id rather than pendent
            f"""
            SELECT DISTINCT l{lognum}_pendent_id FROM vw_logger_deployment 
            WHERE sitecode = '{sitecode}'
            ;""",
            eng
        ) \
        [f'l{lognum}_pendent_id'] \
        .tolist()
    
    return jsonify(pendantids=[x for x in pendantids if not pd.isnull(x)])


    
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