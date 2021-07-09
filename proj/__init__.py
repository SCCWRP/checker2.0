from os import environ
from flask import Flask
from flask_cors import CORS
from sqlalchemy import create_engine

# import blueprints to register them
from .main import homepage
from .match import match_file
from .core.functions import fetch_meta
from .custom.func1 import func1
from .custom.func2 import func2


app = Flask(__name__, static_url_path='/static')
app.debug = True # remove for production

CORS(app)


# does your application require uploaded filenames to be modified to timestamps or left as is
app.config['CORS_HEADERS'] = 'Content-Type'

app.config['MAIL_SERVER'] = '192.168.1.18'

app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB limit
app.secret_key = 'any random string'

# set the database connection string, database, and type of database we are going to point our application at
app.eng = create_engine(environ.get("DB_CONNECTION_STRING"))

# system fields
app.system_fields = [
    'objectid','globalid','created_date','created_user','last_edited_date','last_edited_user','email_login','submissionid','warning'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = []

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
app.excel_offset = 1

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
    'test1': {'tables': ['tbl_test1'], 'function': func1},
    'test2': {'tables': ['tbl_test2'], 'function': func2}
}


app.register_blueprint(homepage)
app.register_blueprint(match_file)
