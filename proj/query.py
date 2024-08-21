import os
import pandas as pd
from io import BytesIO
from flask import request, Blueprint, current_app, send_from_directory, session, render_template, g, make_response, send_file, flash
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
	return send_from_directory(os.path.join(os.getcwd(), 'logs'), path)



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