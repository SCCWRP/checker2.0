from os import environ
from flask import Flask,current_app, g
from sqlalchemy import create_engine

# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .scraper import scraper
from .core.functions import fetch_meta
from .custom.sav import sav #def fcn in .py
from .custom.bruv import bruv #def fcn in .py
from .custom.fishseines import fishseines #def fcn in .py
from .custom.crabtrap import crabtrap
from .custom.vegetation import vegetation #def fcn in .py
#from .custom.nutrients import nutrients_field, nutrients_lab #def fcn in .py
#from .custom.edna import edna_field, edna_lab #def fcn in .py
#from .custom.sedimentgrainsize import sedimentgrainsize_field, sedimentgrainsize_lab #def fcn in .py
from .custom.discretewq import discretewq #def fcn in .py
from .custom.benthicinfauna import benthicinfauna #def fcn in .py
from .custom.feldspar import feldspar #def fcn in .py
#from .custom.logger import logger #def fcn in .py
from .custom.bruv_visual_map import bruv_visual_map
from .custom.sav_visual_map import sav_visual_map
from .custom.veg_visual_map import veg_visual_map
from .custom.fish_visual_map import fish_visual_map

# Dynamic Imports Here

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
app.project_name = environ.get("PROJNAME")

# script root (for any links we put, mainly lookup lists)
app.script_root = 'checker'

# Maintainers
#app.maintainers = ['robertb@sccwrp.org', 'zaibq@sccwrp.org','duyn@sccwrp.org','nataliem@sccwrp.org' ,'minan@sccwrp.org', 'delaramm@sccwrp.org'] #,'pauls@sccwrp.org']
app.maintainers = ['monted97@gmail.com']

# Mail From
app.mail_from = 'admin@checker.sccwrp.org'

# system fields
app.system_fields = [
    'objectid','globalid','created_date','created_user',
    'last_edited_date','last_edited_user',
    'login_email','login_agency','login_datatype','login_estuary','login_startdate','login_enddate','submissionid','warnings'
]

# just in case we want to set aside certain tab names that the application should ignore when reading in an excel file
app.tabs_to_ignore = ['Instructions','glossary','Lookup Lists']

# number of rows to skip when reading in excel files
# Some projects will give templates with descriptions above column headers, in which case we have to skip a row when reading in the excel file
# NESE offsets by 2 rows
app.excel_offset = 0

# data sets / groups of tables for datatypes will be defined here in __init__.py
## (Zaib) Changes to make to the dataset: 
## Split datatypes that have field and lab data templates split (see Teams 'Final Templates')
## into <datatype>meta and <datatype>data for field and lab, respectively. 
## Adjust the functions within the <datatype>.py files to split the field and lab checks. 
## 
app.datasets = {
    # tables
    #   these lists are treated as sets when it does matching.
    #   i think they have to be stored here as lists because sets are not json serializable?
    #   NOTE BE SURE TO PUT THEM IN THE ORDER YOU NEED THEM TO BE LOADED
    # function
    #   the custom checks function associated with the datatype. Imported up top
    'sav':{
        'tables': ['tbl_protocol_metadata','tbl_sav_metadata','tbl_savpercentcover_data'],
        'login_fields': ['login_email','login_agency'],
        'function': sav,
        'map_func': sav_visual_map,
        'spatialtable': 'tbl_sav_metadata'
    },
    
    # --- SOP 2: Water Quality --- #
    'discretewq':{
        'tables': ['tbl_protocol_metadata','tbl_waterquality_metadata','tbl_waterquality_data'],
        'login_fields': ['login_email','login_agency'],
        'function': discretewq
    },
    
    # --- SOP 3: Nutrients --- #
   # 'nutrients_lab':{
   #     'tables': ['tbl_protocol_metadata','tbl_nutrients_labbatch_data','tbl_nutrients_data'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': nutrients_lab
   # },
   # 'nutrients_field':{
   #     'tables': ['tbl_protocol_metadata','tbl_nutrients_metadata'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': nutrients_field
   # },
    
    # --- SOP 4: eDNA --- #
   # 'edna_field':{
   #     'tables': ['tbl_protocol_metadata','tbl_edna_metadata'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': edna_field
   # },
   # 'edna_lab':{
   #     'tables': ['tbl_protocol_metadata','tbl_edna_water_labbatch_data','tbl_edna_sed_labbatch_data','tbl_edna_data'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': edna_lab
   # },
    
    # --- SOP 5: Sediment Grain Size --- #
   # 'sedimentgrainsize_field':{
   #     'tables': ['tbl_protocol_metadata', 'tbl_sedgrainsize_metadata'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': sedimentgrainsize_field
   # },
   # 'sedimentgrainsize_lab':{
   #     'tables': ['tbl_protocol_metadata', 'tbl_sedgrainsize_data', 'tbl_sedgrainsize_labbatch_data'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': sedimentgrainsize_lab
   # },

    #removing tbl_bruv_data since this with be separated as lab data later - zaib 7 oct 2021
    # change to bruvmeta
    'bruv':{
        #'tables': ['tbl_protocol_metadata','tbl_bruv_metadata','tbl_bruv_data'],
        'tables': ['tbl_protocol_metadata','tbl_bruv_metadata'],
        'login_fields': ['login_email','login_agency'],
        'function': bruv,
        'map_func': bruv_visual_map,
        'spatialtable': 'tbl_bruv_metadata'
    },
    # '''
    # 'bruvlab':{
    #     'tables': ['tbl_bruv_data'],
    #     'login_fields': ['login_email','login_agency'],
    #     'function': bruvlab,
    # },
    # '''
    'fishseines':{
        'tables': ['tbl_protocol_metadata','tbl_fish_sample_metadata','tbl_fish_abundance_data','tbl_fish_length_data'],
        'login_fields': ['login_email','login_agency'],
        'function': fishseines,
        'map_func': fish_visual_map,
        'spatialtable': 'tbl_fish_sample_metadata'
    },
    'crabtrap': {
        'tables': ['tbl_protocol_metadata','tbl_crabtrap_metadata','tbl_crabfishinvert_abundance','tbl_crabbiomass_length'], 
        'login_fields': ['login_email','login_agency'],
        'function': crabtrap
    },
    'vegetation':{
        'tables': ['tbl_protocol_metadata','tbl_vegetation_sample_metadata','tbl_vegetativecover_data','tbl_epifauna_data'],
        'login_fields': ['login_email','login_agency'],
        'function': vegetation,
        'map_func': veg_visual_map,
        'spatialtable': 'tbl_vegetation_sample_metadata'
    },
    'benthicinfauna':{
        'tables': ['tbl_protocol_metadata','tbl_benthicinfauna_metadata','tbl_benthicinfauna_labbatch','tbl_benthicinfauna_abundance','tbl_benthicinfauna_biomass'],
        'login_fields': ['login_email','login_agency'],
        'function': benthicinfauna
    },
    'feldspar':{
        'tables': ['tbl_protocol_metadata','tbl_feldspar_metadata','tbl_feldspar_data'],
        'login_fields': ['login_email','login_agency'],
        'function': feldspar
    }#,
   # 'logger':{
   #     'tables': ['tbl_protocol_metadata','tbl_wq_logger_metadata','tbl_logger_ctd_data','tbl_logger_mdot_data','tbl_logger_troll_data','tbl_logger_tidbit_data'],
   #     'login_fields': ['login_email','login_agency'],
   #     'function': logger
   # }
}

# need to assert that the table names are in (SELECT table_name FROM information_schema.tables)

app.register_blueprint(upload)
app.register_blueprint(homepage)
app.register_blueprint(finalsubmit)
app.register_blueprint(download)
app.register_blueprint(scraper)


