from os import environ
from flask import Flask
from flask_cors import CORS
from sqlalchemy import create_engine

# import blueprints to register them
from .main import homepage
from .match import match_file
from .core.functions import fetch_meta



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
    'objectid','globalid','created_date','created_user','last_edited_date','last_edited_user','email_login','submissionid'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = []

# data sets / groups of tables for datatypes will be defined here in __init__.py
app.datasets = {
    # these lists are treated as sets when it does matching.
    # i think they have to be stored here as lists because sets are not json serializable?
    'test1': ['tbl_test1'],
    'test2': ['tbl_test2']
}

app.dbmetadata = {
    tblname: fetch_meta(tblname, app.eng) 
    for tblname in set([y for x in app.datasets.values() for y in x])
}

app.register_blueprint(homepage)
app.register_blueprint(match_file)
