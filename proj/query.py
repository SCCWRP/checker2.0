import json, time, os
import pandas as pd
from io import BytesIO
from flask import request, jsonify, Blueprint, current_app, send_from_directory, url_for, session, render_template, g, make_response, send_file, flash
from sqlalchemy import create_engine, text
from zipfile import ZipFile
from functools import wraps
from datetime import datetime

from .utils.excel import format_existing_excel


def support_jsonp(f):
    """Wraps JSONified output for JSONP"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        callback = request.args.get('callback', False)
        if callback:
            content = str(callback) + '(' + str(f(*args,**kwargs).data) + ')'
            return current_app.response_class(content, mimetype='application/javascript')
        else:
            return f(*args, **kwargs)
    return decorated_function


# Taken from the Bight '18 checker
# A tool for querying the unified bight database

query = Blueprint('unifiedquery', __name__)


@query.route('/logs/<path:path>')
def send_log(path):
	print("log route")
	print(path)
	#send_from_directory("/some/path/to/static", "my_image_file.jpg")
	return send_from_directory(os.path.join(os.getcwd(), 'logs'), path)


@query.route('/unified-query', methods=['GET'])
@support_jsonp
def unifiedquery():
    # function to build query from url string and return result as an excel file or zip file if requesting all data
    print("start export")
    admin_engine = create_engine(os.environ.get("UNIFIED_BIGHT_DB_ADMIN_CONNECTION_STRING"))
    query_engine = create_engine(os.environ.get("UNIFIED_BIGHT_DB_READONLY_CONNECTION_STRING"))

    # initialize this variable
    action = None
    outlink = None

    hostname = request.host

    print("hostname")
    print(hostname)

    # sql injection check one
    def cleanstring(instring):
        # unacceptable characters from input - removed & ampersand it was interferring with Bays & Harbors - 4mar22
        special_characters = '''!-[]{};:'"\,<>./?@#$^*~'''

        # remove punctuation from the string
        outstring = ""
        for char in instring:
            if char not in special_characters:
                outstring = outstring + char
        return outstring

    # sql injection check two
    def exists_table(local_engine, local_table_name):
        local_table_name = local_table_name.strip()
        # check lookups
        lsql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG=%s"
        lquery = local_engine.execute(lsql, ("unified"))
        lresult = lquery.fetchall()
        lresult = [r for r, in lresult]
        result = lresult
        if local_table_name in result:
            print("found matching table")
            return 1
        else:
            print("no matching table return empty")
            return 0

    # if request.args.get("action"):
    gettime = int(time.time())
    TIMESTAMP = str(gettime)
    export_file = os.path.join(os.getcwd(), 'logs', f'{TIMESTAMP}-export.csv')
    export_link = os.path.join(hostname, url_for('unifiedquery.send_log', path=f'{TIMESTAMP}-export.csv'))
    export_link = os.path.join(hostname, url_for('unifiedquery.send_log', path=f'{TIMESTAMP}-export.csv'))

    # sql injection check three
    valid_tables = {'chemistry': 'tbl_chemistry', 'benthicinfauna': 'tbl_benthicinfauna',
                    'sqoscores': 'tbl_sqoscores', 'sqocondition': 'tbl_sqocondition', 'sqosummary': 'tbl_sqosummary'}
    if request.args.get("callback"):
        test = request.args.get("callback", False)
        print(test)
    if request.args.get("action"):
        action = request.args.get("action", False)
        print(action)
        cleanstring(action)
        print(action)
    if request.args.get("query"):
        # doesnt handle urls in query parameter if it has an & - the ampersand treats it as another parameter - 4mar22
        query = request.args.get("query", False)
        print(query)
        jsonstring = json.loads(query)

    if action == "multiple":
        print("enter multiple routine")
        outlink = 'https://{}{}'.format(hostname, os.path.join(url_for('unifiedquery.send_log', path=f'{TIMESTAMP}-export.zip')))
        zipfile = os.path.join(os.getcwd(), 'logs', f'{TIMESTAMP}-export.zip')
        with ZipFile(zipfile, 'w') as zip:
            for item in jsonstring:
                print("for loop %s" % item)
                table_name = jsonstring[item]['table']
                table_name = table_name.replace("-", "_")
                # check table_name for prevention of sql injection
                cleanstring(table_name)
                print("table_name")
                print(table_name)
                table = valid_tables[table_name]
                print("table")
                print(table)
                check = exists_table(admin_engine, table)
                if table_name in valid_tables and check == 1:
                    sql = jsonstring[item]['sql']
                    # check sql string - clean it of any special characters
                    cleanstring(sql)
                    outputfilename = f'{table_name}-export.csv'
                    outputfile = os.path.join(os.getcwd(), 'logs', outputfilename)
                    print("sql")
                    print(sql)
                    isql = text(sql)
                    print("DEBUG HERE:")
                    print(isql)
                    rsql = query_engine.execute(isql)
                    df = pd.DataFrame(rsql.fetchall())
                    if len(df) > 0:
                        df.columns = rsql.keys()
                        df.columns = [x.lower() for x in df.columns]
                        
                        df.to_csv(outputfile, header=True,
                                  index=False, encoding='utf-8')
                        print("outputfile")
                        print(outputfile)
                        print("outputfilename")
                        print(outputfilename)
                        print("write multiple to zip")
                        zip.write(outputfile, outputfilename)
                # if we dont pass validation then something is wrong - just error out
                else:
                    response = jsonify({'code': 200, 'link': export_link})
                    return response

    if action == "single":
        for item in jsonstring:
            table_name = jsonstring[item]['table']
            outlink = 'https://{}{}'.format( hostname, os.path.join( url_for('unifiedquery.send_log', path=f'{TIMESTAMP}-{table_name}-export.csv')))
            csvfile = os.path.join(os.getcwd(), 'logs', f'{TIMESTAMP}-{table_name}-export.csv')
            table_name = table_name.replace("-", "_")
            # check table_name for prevention of sql injection
            cleanstring(table_name)
            table = valid_tables[table_name]
            print(table)
            check = exists_table(admin_engine, table)
            if table_name in valid_tables and check == 1:
                sql = jsonstring[item]['sql']
                # check sql string - clean it of any special characters
                cleanstring(sql)
                outputfilename = f'{TIMESTAMP}-{table_name}-export.csv'
                outputfile = os.path.join(os.getcwd(), 'logs', outputfilename)
                isql = text(sql)
                rsql = query_engine.execute(isql)
                df = pd.DataFrame(rsql.fetchall())
                if len(df) > 0:
                    df.columns = rsql.keys()
                    df.columns = [x.lower() for x in df.columns]
                    df.to_csv(outputfile, header=True,
                              index=False, encoding='utf-8')
                else:
                    response = jsonify({'code': 200, 'link': export_link})
                    return response

    export_link = outlink
    admin_engine.dispose()
    query_engine.dispose()
    response = jsonify({'code': 200, 'link': export_link})
    return response




@query.route('/query')
def qrydata():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")

    datasets = current_app.datasets
    dataset = request.args.get("dataset")
    analysis_tables = request.args.get("analysis_tables")
    additional_tables = request.args.get("additional_tables")
    
    if dataset is None:
        return render_template('query.jinja2', datasets = datasets, authorized = authorized, project_name = current_app.project_name, background_image = current_app.config.get("BACKGROUND_IMAGE"))
    
    if dataset not in datasets.keys():
        flash(f"Dataset {dataset} not found")
        return render_template('query.jinja2', datasets = datasets, authorized = authorized, project_name = current_app.project_name, background_image = current_app.config.get("BACKGROUND_IMAGE"))
        
    excel_blob = BytesIO()
    
    with pd.ExcelWriter(excel_blob) as writer:
        
        alltables = [
            *datasets.get(dataset).get("tables", []), 
            *(datasets.get(dataset).get("analysis_tables", []) if analysis_tables is not None else []),
            *(datasets.get(dataset).get("additional_tables", [])  if additional_tables is not None else [])
        ]
        
        for tbl in alltables:
            print(f"QUERYING DATA FOR {tbl}")
            tmpdf = pd.read_sql(f"SELECT * FROM {tbl};", g.eng)
            colorder = pd.read_sql(f"SELECT column_name FROM column_order WHERE table_name = '{tbl}' ORDER BY custom_column_position;", g.eng).column_name.tolist()
            
            if len(colorder) > 0:
                tmpdf = tmpdf[colorder]
                
            tmpdf.to_excel(writer, sheet_name = tbl, index = False)
        
    excel_blob.seek(0)
    
    # apply formatting
    print("# apply formatting")
    formatted_blob = format_existing_excel(excel_blob)
    
    # Make a response object to set a custom cookie
    resp = make_response(send_file(formatted_blob, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', download_name=f"""{datasets.get(dataset).get('label')}_{datetime.now().strftime('%Y-%m-%d')}_DATA-EXPORT.xlsx"""))

    # Set a cookie to let browser know that the file has been sent
    resp.set_cookie('file_sent', 'true', max_age=1)

    print("End Data Query")
    
    return resp