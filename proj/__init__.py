from os import environ
from flask import Flask
from sqlalchemy import create_engine

# import blueprints to register them
from .main import homepage
from .load import finalsubmit
from .download import download
from .core.functions import fetch_meta
from .custom.datalogger import datalogger


app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production


# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'

app.config['MAIL_SERVER'] = '192.168.1.18'

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = environ.get("FLASK_APP_SECRET_KEY")

# set the database connection string, database, and type of database we are going to point our application at
app.eng = create_engine(environ.get("DB_CONNECTION_STRING"), echo=True)

# Project name
app.project_name = "NESE"

# Maintainers
app.maintainers = ['robertb@sccwrp.org', 'duyn@sccwrp.org']

# Mail From
app.mail_from = 'admin@checker.sccwrp.org'

# system fields
app.system_fields = [
    'objectid','globalid','created_date','created_user',
    'last_edited_date','last_edited_user',
    'login_email','login_agency','submissionid','warnings'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = []

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = 2

# data sets / groups of tables for datatypes will be defined here in __init__.py
app.datasets = {
    # tables
    #   these lists are treated as sets when it does matching.
    #   i think they have to be stored here as lists because sets are not json serializable?
    # function
    #   the custom checks function associated with the datatype. Imported up top
    # offset
    #   Some people like to give the clients excel submission templates with column descriptions in the first row
    #   offset refers to the number of rows to offset, or skip, when reading in the excel file
    'datalogger': {'tables': ['tbl_data_logger_raw', 'tbl_data_logger_metadata'], 'function': datalogger},
    'dataloggerraw': {'tables': ['tbl_data_logger_raw'], 'function': datalogger}
}

# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)


app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
