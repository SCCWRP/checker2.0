from os import environ
from flask import Flask, g
from sqlalchemy import create_engine

# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .scraper import scraper
from .core.functions import fetch_meta
from .custom.bmpmeta import meta
from .custom.bmpmonitoring import monitoring

app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production


# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'

app.config['MAIL_SERVER'] = '192.168.1.18'

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = environ.get("FLASK_APP_SECRET_KEY")

# set the database connection string, database, and type of database we are going to point our application at
#app.eng = create_engine(environ.get("DB_CONNECTION_STRING"))
def connect_db():
    return create_engine(environ.get("DB_CONNECTION_STRING"))

@app.before_request
def before_request():
    g.eng = connect_db()

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'eng'):
        g.eng.dispose()

# Project name
app.project_name = "BMP"

# script root (for any links we put, mainly lookup lists)
app.script_root = 'checker2'

# Maintainers
app.maintainers = ['robertb@sccwrp.org', 'zaibq@sccwrp.org','duyn@sccwrp.org','pauls@sccwrp.org']

# Mail From
app.mail_from = 'admin@checker.sccwrp.org'

# system fields
app.system_fields = [
    'objectid','globalid','created_date','created_user',
    'last_edited_date','last_edited_user',
    'login_email','login_agency','login_datatype','login_dataprovider', 'dataprovider','submissionid','warnings',
    'login_testsite', 'msid', 'siteid'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = []

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = 1

# data sets / groups of tables for datatypes will be defined here in __init__.py
app.datasets = {
    # tables
    #   these lists are treated as sets when it does matching.
    #   i think they have to be stored here as lists because sets are not json serializable?
    #   NOTE BE SURE TO PUT THEM IN THE ORDER YOU NEED THEM TO BE LOADED
    # function
    #   the custom checks function associated with the datatype. Imported up top
    'meta': {
        'tables': ['tbl_testsite','tbl_watershed','tbl_bmpinfo','tbl_monitoringstation'], 
        'login_fields': ['login_email','login_dataprovider'],
        'function': meta
    },
    'monitoring': {
        'tables': ['tbl_precipitation','tbl_ceden_waterquality','tbl_flow'],
        'login_fields': ['login_email','login_dataprovider','login_testsite'],
        'function': monitoring
    }
}

# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)

app.register_blueprint(upload)
app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
app.register_blueprint(scraper)



