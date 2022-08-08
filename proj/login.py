import time, os
import pandas as pd
from flask import session, Blueprint, current_app, request, render_template, jsonify, g
from .utils.exceptions import default_exception_handler
from .utils.login import get_login_field

homepage = Blueprint('homepage', __name__)
#projName = os.environ["PROJNAME"]
@homepage.route('/', methods = ['GET', 'POST', 'DELETE'])
def index():
    eng = g.eng

    # upon new request clear session, reset submission ID, reset submission directory
    # Hold off for now, trying new login system - 8/8/2022
    if request.method == 'DELETE':
        session.clear()
        return jsonify(msg="Session info cleared")

    if session.get('login_info') :
        login_info = dict()
        for k in session.get('login_info').keys():
            # Here we are essentially renaming keys of dictionary
            login_info[str(k).replace('login_','').capitalize()] = session.get('login_info').get(k)

        return render_template('index.html', login_info = login_info)

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
    
    return render_template('index.html', projectname = current_app.project_name, dtypes = current_app.datasets, login_info = False )


@homepage.route('/login_values')
def login_values():
    eng = g.eng
    print("request.args")
    print(request.args)

    # request.args is an immutable dictionary, we turn it into a regular dictionary to use the "pop" method
    args = dict(request.args)

    assert 'table' in args.keys(), 'in login_values: table not specified in query string args'
    assert 'displayfield' in args.keys(), 'in login_values: displayfield not specified in query string args'
    assert 'valuefield' in args.keys(), 'in login_values: valuefield not specified in query string args'
    
    table = args.pop('table')
    displayfield = args.pop('displayfield')
    valuefield = args.pop('valuefield')

    data = get_login_field(
        table,
        displayfield,
        valuefield,
        **args
    )
    return jsonify(data = data)


@homepage.route('/login', methods = ['GET','POST'])
def login():
    
    login_info = dict(request.form)
    print("login_info")
    print(login_info)
    # date_range = login_info['login_startdate'].split('_')
    # login_info['login_startdate'] = date_range[0]
    # login_info['login_enddate'] = date_range[1]


    # Something that may or may not be specific for this project, but
    # based on the dataset, there are different login fields that are relevant
    # assert login_info.get('login_datatype') in current_app.datasets.keys(), f"login_datatype form field value {login_info.get('login_datatype')} not found in current_app.datasets.keys()"
    session['login_info'] = login_info
    
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
