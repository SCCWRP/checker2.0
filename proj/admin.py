import os, re
import pandas as pd
from bs4 import BeautifulSoup
from io import BytesIO
from flask import Blueprint, g, current_app, render_template, redirect, url_for, session, request, jsonify, send_file

import psycopg2
from psycopg2 import sql
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from .utils.db import metadata_summary

admin = Blueprint('admin', __name__)

@admin.route('/track')
def tracking():
    print("start track")
    sql_session =   '''
                    SELECT LOGIN_EMAIL,
                        LOGIN_AGENCY,
                        SUBMISSIONID,
                        DATATYPE,
                        SUBMIT,
                        CREATED_DATE,
                        ORIGINAL_FILENAME
                    FROM SUBMISSION_TRACKING_TABLE
                    WHERE SUBMISSIONID IS NOT NULL
                        AND ORIGINAL_FILENAME IS NOT NULL
                    ORDER BY CREATED_DATE DESC
                    '''
    session_results = g.eng.execute(sql_session)
    session_json = [dict(r) for r in session_results]
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    if not authorized:
        return render_template('admin_password.html', redirect_route='track')

    
    # session is a reserved word in flask - renaming to something different
    return render_template('track.html', session_json=session_json, authorized=authorized)


@admin.route('/schema')
def schema():
    print("entering schema")

    # This is kind of obsolete - orgiinally i was going to have this only available to scientists
    # We will keep this because later we will have different levels of access and privileges
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")

    print("start schema information lookup routine")
    eng = g.eng

    # Query string arg to get the specific datatype
    datatype = request.args.get("datatype")

    # Query string arg option to download
    download = str(request.args.get("download")).strip().lower() == 'true'
    
    # If a specific datatype is selected then display the schema for it
    if datatype is not None:
        if datatype not in current_app.datasets.keys():
            return f"Datatype {datatype} not found"

        # dictionary to return
        tbl_column_info = {}
        table_descriptions = {}
        
        tables = current_app.datasets.get(datatype).get("tables")
        for tbl in tables:
            df = metadata_summary(tbl, eng)
            
            df['lookuplist_table_name'] = df['lookuplist_table_name'].apply(
                lambda x: f"""<a target=_blank href=/{current_app.script_root}/scraper?action=help&layer={x}>{x}</a>""" if pd.notnull(x) else ''
            )

            # drop "table_name" column
            df.drop('tablename', axis = 'columns', inplace = True)

            # drop system fields
            df.drop(df[df.column_name.isin(current_app.system_fields)].index, axis = 'rows', inplace = True)

            df.fillna('', inplace = True)


            try:
                # Prepare and execute the query
                sql_query = text("SELECT tabledescription FROM table_descriptions WHERE tablename = :tbl") 
                tbldesc_query_result = pd.read_sql(sql_query, eng, params={"tbl": tbl}).tabledescription.values
                tbldesc = tbldesc_query_result[0] if len(tbldesc_query_result) > 0 else ''
            except ProgrammingError as e:
                print(f"An error occurred: {e}")
                raise Exception(f"Error occurred getting table description for {tbl}: Most likely the table_descriptions table doesnt exist.\n{e}")

            # This ensures that the keys of each dictionary always match - this will be useful for the jinja template
            tbl_column_info[tbl] = df.to_dict('records')
            table_descriptions[tbl] = tbldesc

        
        if download:
            excel_blob = BytesIO()

            with pd.ExcelWriter(excel_blob) as writer:
                for key in tbl_column_info.keys():
                    df_to_download = pd.DataFrame.from_dict(tbl_column_info[key])
                    df_to_download['lookuplist_table_name'] = df_to_download['lookuplist_table_name'].apply(
                        lambda x: "https://{}/{}/scraper?action=help&layer={}".format(
                            request.host,
                            current_app.config.get('APP_SCRIPT_ROOT'),
                            BeautifulSoup(x, 'html.parser').text.strip()
                        ) if BeautifulSoup(x, 'html.parser').text.strip() != '' else ''
                    )
                    df_to_download.to_excel(writer, sheet_name=key, index=False)

            excel_blob.seek(0)

            # if the query string said "download=true"
            return send_file(
                excel_blob, 
                download_name = f'{datatype}_schema.xlsx', 
                as_attachment = True, 
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

        # Return the datatype query string arg - the template will need access to that
        return render_template('schema.jinja2', metadata=tbl_column_info, datatype=datatype, table_descriptions=table_descriptions, authorized=authorized)
        
    # only executes if "datatypes" not given
    datatypes_list = current_app.datasets.keys()
    return render_template('schema.jinja2', datatypes_list=datatypes_list, authorized=authorized)




@admin.route('/save-changes', methods = ['POST'])
def savechanges():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    if authorized:
        data = request.get_json()

        tablename = str(data.get("tablename")).strip()
        column_name = str(data.get("column_name")).strip()
        column_description = str(data.get("column_description")).strip()



        # connect with psycopg2
        connection = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("PGPASSWORD"),
        )

        connection.set_session(autocommit=True)

        with connection.cursor() as cursor:
            command = sql.SQL(
                """
                COMMENT ON COLUMN {tablename}.{column_name} IS {description};
                """
            ).format(
                tablename = sql.Identifier(tablename),
                column_name = sql.Identifier(column_name),
                description = sql.Literal(column_description)
            )
            
            cursor.execute(command)

        connection.close()

        
        return jsonify(message=f"successfully updated comment on the column {column_name} in the table {tablename}")

    return ''


@admin.route('/save-description', methods = ['POST'])
def savetabledescription():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    
    if authorized:
        data = request.get_json()

        tablename = str(data.get("tablename")).strip()
        
        tabledescription = str(data.get("tabledescription")).strip()

        # connect with psycopg2
        connection = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("PGPASSWORD"),
        )

        connection.set_session(autocommit=True)

        with connection.cursor() as cursor:
            command = sql.SQL(
                """
                INSERT INTO table_descriptions (tablename, tabledescription) 
                    VALUES ({tablename}, {tabledescription}) 
                    ON CONFLICT ON CONSTRAINT table_descriptions_pkey 
                    DO UPDATE SET tabledescription = EXCLUDED.tabledescription;
                """
            ).format(
                tablename = sql.Literal(tablename),
                tabledescription = sql.Literal(tabledescription)
            )
            
            cursor.execute(command)

        connection.close()

        return jsonify(message=f"successfully updated description for the table {tablename}")

    return ''


@admin.route('/column-order', methods = ['GET','POST'])
def column_order():
    authorized = session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")
    if not authorized:
        # return template for GET request, empty string for everything else
        return render_template('admin_password.html', redirect_route='column-order') \
            if request.method == 'GET' \
            else ''
    

    # connect with psycopg2
    connection = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("PGPASSWORD"),
    )

    connection.set_session(autocommit=True)

    if request.method == 'GET':
        eng = g.eng

        # update column-order table based on contents of information schema
        cols_to_add_qry = (
            """
            WITH cols_to_add AS (
                SELECT 
                    table_name,
                    column_name,
                    ordinal_position AS original_db_position,
                    ordinal_position AS custom_column_position 
                FROM
                    information_schema.COLUMNS 
                WHERE
                    table_name IN ( SELECT DISTINCT table_name FROM column_order ) 
                    AND ( table_name, column_name ) NOT IN ( SELECT DISTINCT table_name, column_name FROM column_order )
            )
            INSERT INTO 
                column_order (table_name, column_name, original_db_position, custom_column_position) 
                (
                    SELECT table_name, column_name, original_db_position, custom_column_position FROM cols_to_add
                )
            ;
            """
        )

        # remove records from column order if they are not there anymore
        cols_to_delete_qry = (
            """
            WITH cols_to_delete AS (
                SELECT TABLE_NAME
                    ,
                    COLUMN_NAME,
                    original_db_position,
                    custom_column_position 
                FROM
                    column_order 
                WHERE
                    TABLE_NAME NOT IN ( SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS ) 
                    OR ( TABLE_NAME, COLUMN_NAME ) NOT IN ( SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS ) 
                ) 
                DELETE FROM column_order 
                WHERE
                    ( TABLE_NAME, COLUMN_NAME ) IN ( SELECT TABLE_NAME, COLUMN_NAME FROM cols_to_delete );
            ;
            """
        )
        with connection.cursor() as cursor:
            command = sql.SQL(cols_to_add_qry)
            cursor.execute(command)
            command = sql.SQL(cols_to_delete_qry)
            cursor.execute(command)

        basequery = (
            """
            WITH baseqry AS (
                SELECT table_name, column_name, custom_column_position FROM column_order ORDER BY table_name, custom_column_position
            )
            SELECT * FROM baseqry
            """
        )
        
        # Query string arg to get the specific datatype
        datatype = request.args.get("datatype")
        
        # If a specific datatype is selected then display the schema for it
        if datatype is not None:
            if datatype not in current_app.datasets.keys():
                return f"Datatype {datatype} not found"

            # dictionary to return
            return_object = {}
            
            tables = current_app.datasets.get(datatype).get("tables")
            for tbl in tables:
                df = pd.read_sql(f"{basequery} WHERE table_name = '{tbl}';", eng)

                df.fillna('', inplace = True)

                return_object[tbl] = df.to_dict('records')
            
            # Return the datatype query string arg - the template will need access to that
            return render_template('column-order.jinja2', metadata=return_object, datatype=datatype, authorized=authorized)
        
        # only executes if "datatypes" not given
        datatypes_list = current_app.datasets.keys()
        return render_template('column-order.jinja2', datatypes_list=datatypes_list, authorized=authorized)
        
    elif request.method == 'POST':
        try:
            data = request.get_json()

            tablename = str(data.get("tablename")).strip()
            column_order_information = data.get("column_order_information")

            with connection.cursor() as cursor:
                for item in column_order_information:
                    column_name = item.get('column_name')
                    column_position = item.get('column_position')
                    command = sql.SQL(
                        """
                        UPDATE column_order 
                            SET custom_column_position = {pos} 
                        WHERE 
                            column_order.table_name = {tablename} 
                            AND column_order.column_name = {column_name};
                        """
                    ).format(
                        pos = sql.Literal(column_position),
                        tablename = sql.Literal(tablename),
                        column_name = sql.Literal(column_name)
                    )
                    
                    cursor.execute(command)

            connection.close()
            return jsonify(message=f"Successfully updated column order for {tablename}")
        except Exception as e:
            print(e)
            return jsonify(message=f"Error: {str(e)}")

    else:
        return ''

    



@admin.route('/adminauth', methods = ['GET','POST'])
def adminauth():

    # I put a link in the schema page for some who want to edit the schema to sign in
    # I put schema as as query string arg to show i want them to be redirected there after they sign in
    if request.args.get("redirect_to"):
        return render_template('admin_password.html', redirect_route=request.args.get("redirect_to"))

    adminpw = request.get_json().get('adminpw')
    if adminpw == os.environ.get("ADMIN_FUNCTION_PASSWORD"):
        session['AUTHORIZED_FOR_ADMIN_FUNCTIONS'] = True


    return jsonify(message=str(session.get("AUTHORIZED_FOR_ADMIN_FUNCTIONS")).lower())
