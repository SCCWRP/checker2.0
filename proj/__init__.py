import os, json, re
from flask import Flask,current_app, g
#from flask_cors import CORS - disabled paul 9jan23
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
from .admin import admin


CUSTOM_CONFIG_PATH = os.path.join(os.getcwd(), 'proj', 'config')

CONFIG_FILEPATH = os.path.join(CUSTOM_CONFIG_PATH, 'config.json')
assert os.path.exists(CONFIG_FILEPATH), "config.json not found"

CONFIG = json.loads(open(CONFIG_FILEPATH, 'r').read())

assert all([item in CONFIG.keys() for item in ["EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS", "DATASETS"]]), \
    """ "EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS", "DATASETS" not found in the keys of the basic config file """


app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production


#CORS(app) - disabled paul 9jan23
#app.config['CORS_HEADERS'] = 'Content-Type' - disabled paul 9jan23

app.config['MAIL_SERVER'] = CONFIG.get('MAIL_SERVER')

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = os.environ.get("FLASK_APP_SECRET_KEY")

# add all the items from the config file into the app configuration
# we should probably access all custom config items in this way
app.config.update(CONFIG)

# set the database connection string, database, and type of database we are going to point our application at
#app.eng = create_engine(os.environ.get("DB_CONNECTION_STRING"))
def connect_db():
    kwargs = {
        # "fast_executemany":True,
        "pool_pre_ping": True,
        # "keepalives": 1,
        # "keepalives_idle": 30,
        # "keepalives_interval": 5,
        # "keepalives_count": 5
    }
    return create_engine(os.environ.get("DB_CONNECTION_STRING"), **kwargs)

@app.before_request
def before_request():
    g.eng = connect_db()

@app.teardown_request
def teardown_request(exception):
    if hasattr(g, 'eng'):
        g.eng.dispose()

# Project name
app.project_name = CONFIG.get("PROJECTNAME")

# script root (for any links we put, mainly lookup lists)
app.script_root = CONFIG.get('APP_SCRIPT_ROOT')


# Maintainers
app.maintainers = CONFIG.get('MAINTAINERS')

# system fields for all applications
app.system_fields = CONFIG.get('SYSTEM_FIELDS')

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = CONFIG.get('EXCEL_TABS_TO_IGNORE') # if separate tabs for lu's, reflect here

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = CONFIG.get('EXCEL_OFFSET')



# Mail From
app.mail_from =  CONFIG.get('MAIL_FROM')


app.datasets = CONFIG.get('DATASETS')

print("app.datasets")
print(app.datasets)

app.global_login_form = CONFIG.get('GLOBAL_LOGIN_FORM') # may be a nonetype object

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
    if CONFIG.get("GLOBAL_LOGIN_FORM"):
        fields = [f"login_{f.get('fieldname')}" for f in CONFIG.get("GLOBAL_LOGIN_FORM")]
    else:
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
#app.register_blueprint(templater_old)
app.register_blueprint(templater)
app.register_blueprint(admin)

