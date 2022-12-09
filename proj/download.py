import os
from flask import send_file, Blueprint, jsonify, request, g, current_app
from pandas import read_sql
from io import BytesIO

from shareplum import Site
from shareplum import Office365
from shareplum.site import Version

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, download_name = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    filename = request.args.get('filename')
    tablename = request.args.get('tablename')

    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", filename), as_attachment = True, download_name = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", filename)) \
            else jsonify(message = "file not found")
    
    elif tablename is not None:
        eng = g.eng
        valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
        if tablename not in valid_tables:
            return "invalid table name provided in query string argument"
        
        data = read_sql(f"SELECT * FROM {tablename};", eng)
        data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

        datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

        data.to_csv(datapath, index = False)

        return send_file( datapath, as_attachment = True, download_name = f'{tablename}.csv' )

    else:
        return jsonify(message = "neither a filename nor a database tablename were provided")


@download.route('/template')
def get_template():
    
    username = os.environ.get('MS_USERNAME')
    password = os.environ.get('MS_PASSWORD')
    url = os.environ.get('SHAREPOINT_SITE_URL')
    teamname = os.environ.get('TEAMS_SITE_NAME')
    filename = os.environ.get('TEMPLATE_FILE_NAME')
    sitefolder = os.environ.get('SUBMISSION_TEMPLATE_SITE_FOLDER')

    authcookie = Office365(url, username=username, password=password).GetCookies()
    site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
    print(os.path.join(url, 'sites', teamname))
    print(sitefolder)
    folder = site.Folder(sitefolder)
    # 
    for f in folder.files:
        if f.get('Name') == filename:
            data = BytesIO(folder.get_file(filename))
            return send_file(data, as_attachment = True, download_name = filename, mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        
    return 'Not found'
        
