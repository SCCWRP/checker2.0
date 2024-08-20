import os, json
from flask import Flask, g
from flask_cors import CORS
from sqlalchemy import create_engine


# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .report import report_bp
from .scraper import scraper
from .templater import templater # for dynamic lookup lists called into template before output to user
from .strata_map_check import map_check, getgeojson
from .admin import admin
from .info import info
from .query import query

CUSTOM_CONFIG_PATH = os.path.join(os.getcwd(), 'proj', 'config')


CONFIG_FILEPATH = os.path.join(CUSTOM_CONFIG_PATH, 'config.json')
assert os.path.exists(CONFIG_FILEPATH), "config.json not found"

CONFIG = json.loads(open(CONFIG_FILEPATH, 'r').read())

assert all([item in CONFIG.keys() for item in ["EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS", "DATASETS"]]), \
    """ "EXCEL_OFFSET", "SYSTEM_FIELDS", "EXCEL_TABS_TO_IGNORE", "MAINTAINERS", "DATASETS" not found in the keys of the basic config file """


app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production

# data.sccwrp.org query app wont work without this
CORS(app)

# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'

app.config['MAIL_SERVER'] = CONFIG.get('MAIL_SERVER')

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = os.environ.get("FLASK_APP_SECRET_KEY")

# add all the items from the config file into the app configuration
# we should probably access all custom config items in this way
app.config.update(CONFIG)

# add option to allow empty tables in submissions
# some projects have asked for that, other projects require us to not allow empty tables in submissions
if 'ALLOW_EMPTY_TABLES' in CONFIG.keys():
    assert CONFIG.get('ALLOW_EMPTY_TABLES') in ('True', 'False'), 'ALLOW_EMPTY_TABLES option in app configuration must be "True" or "False" - case sensitive'
    app.config['ALLOW_EMPTY_TABLES'] = eval(CONFIG.get('ALLOW_EMPTY_TABLES'))

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


# Add system fields to system fields table
try:
    print("Create temporary db connection")
    tmpeng = connect_db()
    print("Done creating temporary db connection")

    print("Creating system fields table")
    tmpeng.execute(
        """
        CREATE TABLE IF NOT EXISTS "sde"."system_fields" (
            "fieldname" varchar(255) COLLATE "pg_catalog"."default" NOT NULL PRIMARY KEY
        );
        """
    )
    print("Done creating system fields table")
    system_fields_tuple_string = "('{}')" \
        .format(
            "'), ('".join([str(x).strip().replace(';','').replace("'","").replace('"','') for x in app.system_fields])
        )
    system_fields_delete_tuple_string = "('{}')" \
        .format(
            "', '".join([str(x).strip().replace(';','').replace("'","").replace('"','') for x in app.system_fields])
        )

    system_fields_command = f"""
        INSERT INTO sde.system_fields (fieldname) VALUES {system_fields_tuple_string} 
        ON CONFLICT ON CONSTRAINT system_fields_pkey DO NOTHING
    """
    print("Inserting system fields")
    print(system_fields_command)
    tmpeng.execute(system_fields_command)
    print("DONE inserting system fields")

    print("Remove fields that are no longer there")
    tmpeng.execute(f"DELETE FROM system_fields WHERE fieldname NOT IN {system_fields_delete_tuple_string}")
    print("Done removing fields that are no longer there")

    print("Dispose temprary engine/connection")
    tmpeng.dispose()
    print("Done disposing temprary engine/connection")


except Exception as e:
    print("WARNING: Unable to create and insert system fields into the system fields table")
    print("Here is the error message")
    print(e)


# This we can use for adding the login columns

# It will be better in the future to simply store these in the environment separately
# constring = re.search("postgresql://(\w+):(.+)@(.+):(\d+)/(\w+)", os.environ.get('DB_CONNECTION_STRING')).groups()
# connection = psycopg2.connect(
#     host=constring[2],
#     database=constring[4],
#     user=constring[0],
#     password=constring[1],
# )

# connection.set_session(autocommit=True)

# for datasetname, dataset in app.datasets.items():
#     fields = [f"login_{f.get('fieldname')}" for f in dataset.get('login_fields')]
#     with connection.cursor() as cursor:
#         for fieldname in fields:
#             print("Attempting to add field to submission tracking table")
#             print(fieldname)
#             command = sql.SQL(
#                 """
#                 ALTER TABLE submission_tracking_table ADD COLUMN IF NOT EXISTS {field} VARCHAR(255);
#                 """
#             ).format(
#                 field = sql.Identifier(fieldname),
#             )
            
#             cursor.execute(command)
#             print(dataset)
#             for tablename in dataset.get('tables'):
#                 print(f"Attempting to add login fields to {tablename}")
#                 print(fieldname)
#                 command = sql.SQL(
#                     """
#                     ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {field} VARCHAR(255);
#                     """
#                 ).format(
#                     field = sql.Identifier(fieldname),
#                     table = sql.Identifier(tablename)
#                 )
#                 cursor.execute(command)
            
#             # login fields need to be in the system fields list
#             app.system_fields.append(fieldname)




# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)

app.register_blueprint(upload)
app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
app.register_blueprint(scraper)
app.register_blueprint(templater)
app.register_blueprint(report_bp)
app.register_blueprint(map_check)
app.register_blueprint(getgeojson)
app.register_blueprint(admin)
app.register_blueprint(info)
app.register_blueprint(query)