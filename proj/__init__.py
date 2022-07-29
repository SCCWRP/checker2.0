import os, json
from flask import Flask,current_app, g
from sqlalchemy import create_engine

# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .scraper import scraper
from .templater import templater # for dynamic lookup lists called into template before output to user
from .core.functions import fetch_meta

CUSTOM_CONFIG_PATH = os.path.join(os.getcwd(), 'proj', 'config')

app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production


# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'

app.config['MAIL_SERVER'] = os.environ.get('FLASK_APP_MAIL_SERVER')

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = os.environ.get("FLASK_APP_SECRET_KEY")

# set the database connection string, database, and type of database we are going to point our application at
#app.eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))
def connect_db():
    return create_engine(os.environ.get("DB_CONNECTION_STRING"))

@app.before_request
def before_request():
    g.eng = connect_db()

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'eng'):
        g.eng.dispose()

# Project name
app.project_name = os.environ.get("PROJNAME")

# script root (for any links we put, mainly lookup lists)
app.script_root = os.environ.get('FLASK_APP_SCRIPT_ROOT')

# Maintainers
app.maintainers = [
    'pauls@sccwrp.org',
    'robertb@sccwrp.org'
]

# Mail From
app.mail_from = os.environ.get('FLASK_APP_MAIL_FROM')

# system fields
app.system_fields = [
    'objectid','globalid','created_date','created_user',
    'last_edited_date','last_edited_user',
    'login_email','login_agency','login_datatype','login_estuary','login_startdate','login_enddate','submissionid','warnings'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = ['Instructions','glossary','Lookup Lists', 'Results_Example'] # if separate tabs for lu's, reflect here

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = int(os.environ.get('FLASK_APP_EXCEL_OFFSET'))

# data sets / groups of tables for datatypes will be defined here in __init__.py
## (Zaib) Changes to make to the dataset: 
## Split datatypes that have field and lab data templates split (see Teams 'Final Templates')
## into <datatype>meta and <datatype>data for field and lab, respectively. 
## Adjust the functions within the <datatype>.py files to split the field and lab checks. 
## 
assert os.path.exists(os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json')), \
    f"{os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json')} configuration file not found"
app.datasets = json.loads( open( os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json'), 'r' ).read() )

# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)

app.register_blueprint(upload)
app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
app.register_blueprint(scraper)
app.register_blueprint(templater)

