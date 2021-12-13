import time, os
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from flask import session, Blueprint, current_app, request, render_template, jsonify, g
from .utils.exceptions import default_exception_handler

homepage = Blueprint('homepage', __name__)
#projName = os.environ["PROJNAME"]
@homepage.route('/', methods = ['GET','POST'])
def index():
    eng = g.eng

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
    

    agencies = pd.read_sql("SELECT agencyname, agencycode FROM lu_agency", eng)
    agencies = {a[0]:a[1] for a in agencies.values}
    print(agencies)

    estuaries = pd.read_sql("SELECT DISTINCT estuary_name FROM mobile_estuary_info", eng).estuary_name.to_list()
    notinmsf_estuaries = pd.read_sql(
        """
        SELECT DISTINCT estuary_name FROM mobile_estuary_info
        WHERE estuary_name NOT IN 
        (SELECT DISTINCT(estuary_name) FROM msf_site_metadata)
        """, eng).estuary_name.to_list()
    #estuaries = [x for x in estuaries if x not in fake_estuaries]
    dates = pd.read_sql("SELECT DISTINCT estuary_date FROM mobile_estuary_info", eng).estuary_date.to_list()
    dates = [x.strftime('%m-%d-%Y') for x in dates]
    
    return render_template(
        'index.html', 
        projectname = current_app.project_name,
        agencies = agencies,
        estuaries = estuaries,
        notinmsf_estuaries = notinmsf_estuaries,
        dates = dates
    )




@homepage.route('/login', methods = ['GET','POST'])
def login():
    
    login_info = dict(request.form)

    date_range = login_info['login_startdate'].split('_')
    login_info['login_startdate'] = date_range[0]
    login_info['login_enddate'] = date_range[1]

    print("login_info")
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
    g.eng.execute(
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
    
    #eng = current_app.eng

    estuary_name = request.form.get('login_estuary')

    print("estuary_name")
    print(estuary_name)
    
    startdates = pd.read_sql(
        f"""
            SELECT DISTINCT estuary_date 
            FROM mobile_estuary_info
            WHERE estuary_name IN ('{estuary_name}')
        """,
        g.eng
    ).estuary_date.to_list()

    startdates = [x.strftime("%Y-%m-%d") for x in startdates]
    
    enddates = [x.strftime('%Y-%m-%d') for x in [datetime.datetime.strptime(x, '%Y-%m-%d')  + relativedelta(months=1) for x in startdates]]
    daterange = [x + "_" + y for x,y in zip(startdates,enddates)]
    print("daterange")
    print(daterange)
    return jsonify(startdates = daterange)

    
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
