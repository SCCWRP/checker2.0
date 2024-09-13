import os, json
from flask import Flask, g
from flask_cors import CORS
from sqlalchemy import create_engine, text


# import blueprints to register them
from .main import upload
from .login import homepage
from .load import finalsubmit
from .download import download
from .report import report_bp
from .scraper import scraper
from .templater import templater # for dynamic lookup lists called into template before output to user
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
    assert str(CONFIG.get('ALLOW_EMPTY_TABLES')).lower() in ('true', 'false'), 'ALLOW_EMPTY_TABLES option in app configuration must be "True" or "False" '
    app.config['ALLOW_EMPTY_TABLES'] = eval( str(CONFIG.get('ALLOW_EMPTY_TABLES')).capitalize() ) 


# add option to allow column comments in the data submission templates
if 'INCLUDE_COLUMN_COMMENTS' in CONFIG.keys():
    assert str(CONFIG.get('INCLUDE_COLUMN_COMMENTS')).lower() in ('true', 'false'), 'INCLUDE_COLUMN_COMMENTS option in app configuration must be "True" or "False" '
    app.config['INCLUDE_COLUMN_COMMENTS'] = eval( str(CONFIG.get('INCLUDE_COLUMN_COMMENTS')).capitalize() ) 




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
app.submission_tips_enabled = str(CONFIG.get('SUBMISSION_TIPS_ENABLED')).lower() == 'true' 

print("Be sure not to prefix the login fields with 'login' in the datasets.json config file")


# Metadata initialization
try:

    # ---- System Fields ---- #
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

    # ----- Table Descriptions ----- #
    projectname = str(CONFIG.get("PROJECTNAME")).replace(';','').replace('"','').replace("'","") 
    order66 = text(f"""
        CREATE TABLE IF NOT EXISTS table_descriptions (
            tablename VARCHAR(255) PRIMARY KEY,
            tablealias VARCHAR(255),
            tabledescription VARCHAR(500),
            project VARCHAR(255),
            comments VARCHAR(1000)
        );
         
        INSERT INTO table_descriptions (
            SELECT 
                DISTINCT table_name AS tablename, table_name AS tablealias, NULL AS tabledescription, '{projectname}' AS project, NULL AS comments 
            FROM information_schema.tables
        ) ON CONFLICT (tablename) DO NOTHING;
    """)

    # execute order 66 (create the table descriptions)
    tmpeng.execute(order66)
    
    # (clean it up)
    del order66


    order66 = text(f"""
        CREATE TABLE IF NOT EXISTS column_order (
            table_name VARCHAR(255),
            column_name VARCHAR(255),
            original_db_position INTEGER,
            custom_column_position INTEGER,
            PRIMARY KEY (table_name, column_name)
        );

        INSERT INTO column_order (table_name, column_name, original_db_position, custom_column_position)
            (
                SELECT 
                    table_name,
                    column_name,
                    ordinal_position AS original_db_position,
                    ordinal_position AS custom_column_position
                FROM 
                    information_schema.COLUMNS
            ) 
            ON CONFLICT (table_name, column_name) DO NOTHING
    """)
    # execute order 66 (create the column order table)
    tmpeng.execute(order66)
    del order66



    # --------------- Building the metadata view --------------- #
    # app config datasets
    DATASETS = CONFIG.get('DATASETS')
    PROJECT = CONFIG.get("PROJECTNAME")


    # Complete SQL query with dynamic case statement
    check_view_sql = """
        SELECT 1 FROM pg_views WHERE viewname = 'vw_metadata';
    """

    # Execute the check
    result = tmpeng.execute(text(check_view_sql)).fetchone()

    if result is None:
        
        # Function to build the SQL case statement - for the datatypes column of the view
        def generate_case_statement(datasets):
            case_statement = "CASE \n"
            for _, value in datasets.items():
                
                # error checking
                assert isinstance(value, dict), "Configuration error - check the datasets param"
                assert "tables" in value.keys(), "Configuration error - check the datasets param - tables not found in a dataset"
                if "tables" not in value.keys():
                    print("WARNING: A dataset is missing a label")

                tables = value.get("tables", [])
                label = value.get("label", "")
                if tables and label:
                    table_list = ", ".join(f"'{table}'::name " for table in tables)
                    case_statement += f"    WHEN ((isc.table_name)::name = ANY (ARRAY[{table_list}])) THEN '{label}'::text\n"
            case_statement += "    ELSE NULL::text\nEND AS datatype"
            return case_statement

        vw_metadata_query = text(
            f"""
                CREATE VIEW vw_metadata AS 
                    WITH meta_outer_query AS (
                        WITH meta AS (
                            WITH fkeys AS (
                                SELECT DISTINCT kcu.table_name,
                                    kcu.column_name,
                                    ccu.table_name AS foreign_table_name
                                FROM ((information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kcu ON ((((tc.constraint_name)::name = (kcu.constraint_name)::name) 
                                AND ((tc.table_schema)::name = (kcu.table_schema)::name))))
                                JOIN information_schema.constraint_column_usage ccu ON ((((ccu.constraint_name)::name = (tc.constraint_name)::name) 
                                AND ((ccu.table_schema)::name = (tc.table_schema)::name))))
                                WHERE (((tc.constraint_type)::text = 'FOREIGN KEY'::text) 
                                AND (
                                    ((tc.table_name)::name ~~ 'tbl_%'::text) 
                                    OR ((tc.table_name)::name ~~ 'analysis_%'::text)
                                    OR ((tc.table_name)::name ~~ 'unified_%'::text)
                                ) 
                                AND ((ccu.table_name)::name ~~ 'lu_%'::text))
                            ), 
                            pkey AS (
                                SELECT c.table_name,
                                    c.column_name,
                                    'YES'::text AS primary_key
                                FROM ((information_schema.table_constraints tc
                                JOIN information_schema.constraint_column_usage ccu USING (constraint_schema, constraint_name))
                                JOIN information_schema.columns c ON ((((c.table_schema)::name = (tc.constraint_schema)::name) 
                                AND ((tc.table_name)::name = (c.table_name)::name) 
                                AND ((ccu.column_name)::name = (c.column_name)::name))))
                                WHERE (((tc.constraint_type)::text = 'PRIMARY KEY'::text) 
                                AND (
                                    ((tc.table_name)::name ~~ 'tbl_%'::text) 
                                    OR ((tc.table_name)::name ~~ 'analysis_%'::text)
                                    OR ((tc.table_name)::name ~~ 'unified_%'::text)
                                ))
                            ), 
                            cmt AS (
                                SELECT cols.table_name AS tablename,
                                    cols.column_name,
                                    ( SELECT col_description(c.oid, (cols.ordinal_position)::integer) AS col_description
                                        FROM pg_class c
                                        WHERE ((c.oid = ( SELECT (((('"'::text || (cols.table_name)::text) || '"'::text))::regclass)::oid AS oid)) 
                                        AND (c.relname = (cols.table_name)::name))) AS description
                                FROM information_schema.columns cols
                                WHERE (((cols.table_name)::name ~~ 'tbl_%'::text) OR ((cols.table_name)::name ~~ 'analysis_%'::text) OR ((cols.table_name)::name ~~ 'unified_%'::text) )
                            ), 
                            colorder AS (
                                SELECT column_order.table_name AS tablename,
                                    column_order.column_name,
                                    column_order.custom_column_position AS column_position
                                FROM column_order
                                WHERE ((column_order.table_name)::name = '{{table}}'::name)
                            )
                            SELECT isc.column_name AS field,
                                isc.column_name AS fieldalias,
                                CASE
                                    WHEN ((isc.udt_name)::name = 'varchar'::name) THEN 'Text'::text
                                    WHEN ((isc.udt_name)::name = 'timestamp'::name) THEN 'Date/Time'::text
                                    WHEN ((isc.udt_name)::name = 'numeric'::name) THEN 'Decimal'::text
                                    WHEN ((isc.udt_name)::name = ANY (ARRAY['int2'::name, 'int4'::name])) THEN 'Integer'::text
                                    WHEN ((fkeys.foreign_table_name)::name = 'lu_yesno'::name) THEN 'Yes/No'::text
                                    ELSE ''::text
                                END AS fieldtype,
                                CASE
                                    WHEN ((isc.udt_name)::name = 'varchar'::name) THEN 'String'::text
                                    WHEN ((isc.udt_name)::name = 'timestamp'::name) THEN 'Date'::text
                                    WHEN ((isc.udt_name)::name = 'numeric'::name) THEN 'Double'::text
                                    WHEN ((isc.udt_name)::name = ANY (ARRAY['int2'::name, 'int4'::name])) THEN 'SmallInteger'::text
                                    ELSE ''::text
                                END AS metadatafieldtype,
                                CASE
                                    WHEN (pkey.primary_key = 'YES'::text) THEN 'y'::text
                                    ELSE 'n'::text
                                END AS primarykey,
                                CASE
                                    WHEN ((isc.is_nullable)::text = 'NO'::text) THEN 'y'::text
                                    ELSE 'n'::text
                                END AS required,
                                isc.character_maximum_length AS size,
                                fkeys.foreign_table_name AS lookuplist,
                                cmt.description,
                                '{PROJECT}'::text AS project,
                                {generate_case_statement(DATASETS)}, -- Dynamic CASE block
                                isc.table_name AS tablename,
                                td.tablealias,
                                td.tabledescription,
                                colorder.column_position
                            FROM (((((information_schema.columns isc
                            LEFT JOIN pkey ON ((((isc.column_name)::name = (pkey.column_name)::name) AND ((isc.table_name)::name = (pkey.table_name)::name))))
                            LEFT JOIN fkeys ON ((((isc.column_name)::name = (fkeys.column_name)::name) AND ((isc.table_name)::name = (fkeys.table_name)::name))))
                            LEFT JOIN cmt ON ((((isc.table_name)::name = (cmt.tablename)::name) AND ((isc.column_name)::name = (cmt.column_name)::name))))
                            LEFT JOIN colorder ON ((((isc.table_name)::name = (colorder.tablename)::name) AND ((isc.column_name)::name = (colorder.column_name)::name))))
                            LEFT JOIN table_descriptions td ON (((td.tablename)::text = ((isc.table_name)::name)::text)))
                            WHERE (((isc.table_name)::name ~~ 'tbl_%'::text) OR ((isc.table_name)::name ~~ 'analysis_%'::text)  OR ((isc.table_name)::name ~~ 'unified_%'::text) )
                        )
                        SELECT meta.field,
                            meta.fieldalias,
                            meta.fieldtype,
                            meta.metadatafieldtype,
                            meta.primarykey,
                            meta.required,
                            meta.size,
                            meta.lookuplist,
                            meta.description,
                            meta.project,
                            meta.datatype,
                            meta.tablename,
                            meta.tablealias,
                            meta.tabledescription
                        FROM meta
                        WHERE (NOT ((meta.field)::name IN ( SELECT DISTINCT system_fields.fieldname
                            FROM system_fields)))
                        ORDER BY meta.datatype, meta.tablename, meta.column_position
                    )
                    SELECT row_number() OVER () AS objectid,
                        meta_outer_query.field,
                        meta_outer_query.fieldalias,
                        meta_outer_query.fieldtype,
                        meta_outer_query.metadatafieldtype,
                        meta_outer_query.primarykey,
                        meta_outer_query.required,
                        meta_outer_query.size,
                        meta_outer_query.lookuplist,
                        meta_outer_query.description,
                        meta_outer_query.project,
                        meta_outer_query.datatype,
                        meta_outer_query.tablename,
                        meta_outer_query.tablealias,
                        meta_outer_query.tabledescription
                    FROM meta_outer_query
            """
        )

        # Create metadata view
        tmpeng.execute(vw_metadata_query)

    # Create the template glossary (for data submission templates, based on the metadata view)
    check_view_sql = """
        SELECT 1 FROM pg_views WHERE viewname = 'vw_template_glossary';
    """
    # Execute the check
    result = tmpeng.execute(text(check_view_sql)).fetchone()
    
    if result is None:
        tmpeng.execute(
            text("""
                CREATE VIEW vw_template_glossary AS 
                    WITH meta AS (
                        SELECT vw_metadata.field,
                            vw_metadata.fieldtype,
                            vw_metadata.primarykey,
                            vw_metadata.required,
                            vw_metadata.size,
                            vw_metadata.description,
                            vw_metadata.tablename,
                            vw_metadata.tablealias,
                            vw_metadata.tabledescription
                        FROM vw_metadata
                    )
                    SELECT meta.field,
                        meta.fieldtype,
                        meta.primarykey,
                        meta.required,
                        meta.size,
                        meta.description,
                        meta.tablename,
                        meta.tablealias,
                        meta.tabledescription
                    FROM (meta
                        LEFT JOIN column_order cols ON ((((meta.field)::name = (cols.column_name)::name) AND ((meta.tablename)::name = (cols.table_name)::name))))
                    ORDER BY meta.tablename, cols.custom_column_position
            """)
        )


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
app.register_blueprint(admin)
app.register_blueprint(info)
app.register_blueprint(query)