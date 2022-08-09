import os, json, re
from flask import Flask,current_app, g
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql

# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .scraper import scraper
from .templater import templater # for dynamic lookup lists called into template before output to user


CUSTOM_CONFIG_PATH = os.path.join(os.getcwd(), 'proj', 'config')


BASIC_CONFIG_FILEPATH = os.path.join(CUSTOM_CONFIG_PATH, 'basic-config.json')
assert os.path.exists(BASIC_CONFIG_FILEPATH), "basic-config.json not found"

BASIC_CONFIG = json.loads(open(BASIC_CONFIG_FILEPATH, 'r').read())

assert all([item in BASIC_CONFIG.keys() for item in ["EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS"]]), \
    """ "EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS" not found in the keys of the basic config file """


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
app.maintainers = BASIC_CONFIG.get('MAINTAINERS')

# system fields for all applications
app.system_fields = BASIC_CONFIG.get('SYSTEM_FIELDS')

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = BASIC_CONFIG.get('EXCEL_TABS_TO_IGNORE') # if separate tabs for lu's, reflect here

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = BASIC_CONFIG.get('EXCEL_OFFSET')



# Mail From
app.mail_from = os.environ.get('FLASK_APP_MAIL_FROM')


# data sets / groups of tables for datatypes will be defined in datasets.json in the proj/config folder
assert os.path.exists(os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json')), \
    f"{os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json')} configuration file not found"
app.datasets = json.loads( open( os.path.join(CUSTOM_CONFIG_PATH, 'datasets.json'), 'r' ).read() )
print("app.datasets")
print(app.datasets)

print("Be sure not to prefix the login fields with 'login' in the datasets.json config file")

# This we can use for adding the login columns

# It will be better in the future to simply store these in the environment separately
constring = re.search("postgresql://(\w+):(.+)@(.+):(\d+)/(\w+)", os.environ.get('DB_CONNECTION_STRING')).groups()
connection = psycopg2.connect(
    host=constring[2],
    database=constring[4],
    user=constring[0],
    password=constring[1],
)

connection.set_session(autocommit=True)

for datasetname, dataset in app.datasets.items():
    fields = [f"login_{f.get('fieldname')}" for f in dataset.get('login_fields')]
    with connection.cursor() as cursor:
        for fieldname in fields:
            print("Attempting to add field to submission tracking table")
            print(fieldname)
            command = sql.SQL(
                """
                ALTER TABLE submission_tracking_table ADD COLUMN IF NOT EXISTS {field} VARCHAR(255);
                """
            ).format(
                field = sql.Identifier(fieldname),
            )
            
            cursor.execute(command)
            print(dataset)
            for tablename in dataset.get('tables'):
                print(f"Attempting to add login fields to {tablename}")
                print(fieldname)
                command = sql.SQL(
                    """
                    ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {field} VARCHAR(255);
                    """
                ).format(
                    field = sql.Identifier(fieldname),
                    table = sql.Identifier(tablename)
                )
                cursor.execute(command)
            
            # login fields need to be in the system fields list
            app.system_fields.append(fieldname)




# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)

app.register_blueprint(upload)
app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
app.register_blueprint(scraper)
app.register_blueprint(templater)

