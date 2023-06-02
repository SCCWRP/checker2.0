import os, json
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template, url_for
#from flask_cors import CORS, cross_origin - disabled paul 9jan23
from pandas import read_sql, DataFrame
from sqlalchemy import text
from zipfile import ZipFile
import time, datetime
import functools
from io import BytesIO
import subprocess as sp
import pandas as pd
#from functools import wrap

def support_jsonp(f):
        """Wraps JSONified output for JSONP"""
        #@wraps(f)
        def decorated_function(*args, **kwargs):
            callback = request.args.get('callback', False)
            if callback:
                content = str(callback) + '(' + str(f(*args,**kwargs).data) + ')'
                return current_app.response_class(content, mimetype='application/javascript')
            else:
                return f(*args, **kwargs)
        return decorated_function

download = Blueprint('download', __name__)

#CORS(download)
#cors = CORS(download, resources={r"/exportdata": {"origins": "*"}})

@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, download_name = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    filename = request.args.get('filename')
    tablename = request.args.get('tablename')

    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", filename), as_attachment = True, download_name = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", filename)) \
            else jsonify(message = "file not found")
    
    elif tablename is not None:
        eng = g.eng
        valid_tables = read_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl%%';", g.eng).values
        
        if tablename not in valid_tables:
            return "invalid table name provided in query string argument"
        
        data = read_sql(f"SELECT * FROM {tablename};", eng)
        data.drop( set(data.columns).intersection(set(current_app.system_fields)), axis = 1, inplace = True )

        datapath = os.path.join(os.getcwd(), "export", "data", f'{tablename}.csv')

        data.to_csv(datapath, index = False)

        return send_file( datapath, as_attachment = True, download_name = f'{tablename}.csv' )

    else:
        return jsonify(message = "neither a filename nor a database tablename were provided")


@download.route('/exportdata', methods=['GET'])
@support_jsonp
#@cross_origin() - disabled paul 9jan23
def exportdata():
        eng = g.eng
        # function to build query from url string and return result as an excel file or zip file if requesting all data
        print("start exportdata")
        # sql injection check one
        def cleanstring(instring):
            # unacceptable characters from input
            special_characters = '''!-[]{};:'"\,<>./?@#$^&*~'''
            # remove punctuation from the string
            outstring = ""
            for char in instring:
                if char not in special_characters:
                    outstring = outstring + char
            return outstring

        # sql injection check two
        def exists_table(local_engine, local_table_name):
            # check lookups
            lsql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG=%s"
            lquery = local_engine.execute(lsql, ("empa2021"))
            lresult = lquery.fetchall()
            lresult = [r for r, in lresult]
            result = lresult 
            if local_table_name in result:
                print("found matching table")
                return 1
            else:
                print("no matching table return empty")
                return 0

        #if request.args.get("action"):
        gettime = int(time.time())
        TIMESTAMP = str(gettime)

        # ---------------------------------------- NOTE ------------------------------------------------ #
        # @Paul as a heads up - with these file paths, the empa directory doesnt exist in the container  # 
        # the bind mount maps /var/www/empa/checker to /var/www/checker in the docker container          #
        # ---------------------------------------------------------------------------------------------- #
        
        #export_file = '/var/www/checker/logs/%s-export.csv' % TIMESTAMP
        export_file = os.path.join(os.getcwd(), "logs", f'{TIMESTAMP}-export.csv')
        export_link = 'https://empachecker.sccwrp.org/checker/logs?filename=%s-export.csv' % TIMESTAMP

        # sql injection check three
        valid_tables = {
            'algaecover': 'tbl_algaecover_data',
            'macroalgaesamplemetadata':'tbl_macroalgae_sample_metadata',
            'algaecoverdata': 'tbl_algaecover_data',
            'floatingdata': 'tbl_floating_data',
            'benthicmetadata': 'tbl_benthicinfauna_metadata',
            'benthiclabbatch':'tbl_benthic_infauna_labbatch_data',
            'benthicabundance': 'tbl_benthic_infauna_abundance_data',
            'benthicbiomass':'tbl_benthic_infauna_biomass_data',
            'bruvmetadata': 'tbl_bruv_metadata',
            'bruvdata':'tbl_bruv_data',
            'crabbiomass': 'tbl_crabbiomass_length',
            'crababundance': 'tbl_crabfishinvert_abundance',
            'crabtrapmetadata':'tbl_crabtrap_metadata',
            'waterqualitymetadata':'tbl_waterquality_metadata',
            'waterqualitydata':'tbl_waterquality_data',
            'ednametadata':'tbl_edna_metadata',
            'ednawaterlabbatch':'tbl_edna_water_labbatch_data',
            'ednasedlabbatch':'tbl_edna_sed_labbatch_data',
            'ednadata':'tbl_edna_data',
            'feldsparmetadata':'tbl_feldspar_metadata',
            'feldspardata':'tbl_feldspar_data',
            'fishsamplemetadata':'tbl_fish_sample_metadata',
            'fishabundance': 'tbl_fish_abundance_data', 
            'fishlength':'tbl_fish_length_data', 
            'loggermetadata':'tbl_wq_logger_metadata',
            'loggerdata':'tbl_wqlogger',
            'nutrientsmetadata':'tbl_nutrients_metadata',
            'nutrientslabbatch':'tbl_nutrients_labbatch_data',
            'nutrientsdata':'tbl_nutrients_data',
            'savmetadata': 'tbl_sav_metadata',
            'savdata':'tbl_savpercentcover_data',
            'sedgrainsizedata':'tbl_sedgrainsize_data', 
            'sedgrainsizelabbatch':'tbl_sedgrainsize_labbatch_data',
            'sedgrainsizemetadata':'tbl_sedgrainsize_metadata',
            'vegetationsamplemetadata':'tbl_vegetation_sample_metadata',
            'vegetationcoverdata':'tbl_vegetativecover_data',
            'epifauna':'tbl_epifauna_data'
        }
        if request.args.get("callback"):
            test = request.args.get("callback", False)
            print(test)
        if request.args.get("action"):
            action = request.args.get("action", False)
            print(action)
            cleanstring(action)
            print(action)
        if request.args.get("query"):
            query = request.args.get("query", False)
            print(query)
            # incoming json string 
            jsonstring = json.loads(query)

        if action == "multiple":
            outlink = 'https://empachecker.sccwrp.org/checker/logs?filename=%s-export.zip' % (TIMESTAMP)
            #zipfile = '/var/www/checker/logs/%s-export.zip' % (TIMESTAMP)
            zipfile = os.path.join(os.getcwd(), "logs", f'{TIMESTAMP}-export.zip')
            with ZipFile(zipfile,'w') as zip:
                for item in jsonstring:
                    table_name = jsonstring[item]['table']
                    table_name = table_name.replace("-", "_")
                    # check table_name for prevention of sql injection
                    cleanstring(table_name)
                    print(table_name)
                    table = valid_tables[table_name]
                    print(table)
                    check = exists_table(g.eng, table)
                    if (table_name in valid_tables) and check == 1:
                        sql = jsonstring[item]['sql']
                        # check sql string - clean it of any special characters
                        cleanstring(sql)
                        outputfilename = '%s-export.csv' % (table_name)
                        outputfile = '/var/www/checker/logs/%s' % (outputfilename)
                        isql = text(sql)
                        print(isql)
                        rsql = g.eng.execute(isql)
                        df = DataFrame(rsql.fetchall())
                        if len(df) > 0:
                            df.columns = rsql.keys()
                            df.columns = [x.lower() for x in df.columns]
                            print(df)
                            df.to_csv(outputfile,header=True, index=False, encoding='utf-8')
                            print("outputfile")
                            print(outputfile)
                            print("outputfilename")
                            print(outputfilename)
                            zip.write(outputfile,outputfilename) 
                    # if we dont pass validation then something is wrong - just error out
                    else:
                        response = jsonify({'code': 200,'link': outlink})
                        return response

        if action == "single":
            for item in jsonstring:
                table_name = jsonstring[item]['table']
                #csvfile = '/var/www/checker/logs/%s-%s-export.csv' % (TIMESTAMP,table_name)
                csvfile = os.path.join(os.getcwd(), "logs", f'{TIMESTAMP}-export.csv')
                table_name = table_name.replace("-", "_")
                outlink = 'https://empachecker.sccwrp.org/checker/logs?filename=%s-%s-export.csv' % (TIMESTAMP,table_name)
                # check table_name for prevention of sql injection
                cleanstring(table_name)
                table = valid_tables[table_name]
                print(table)
                check = exists_table(eng, table)
                if table_name in valid_tables and check == 1:
                    sql = jsonstring[item]['sql']
                    # check sql string - clean it of any special characters
                    cleanstring(sql)
                    outputfilename = '%s-%s-export.csv' % (TIMESTAMP,table_name)
                    #outputfile = '/var/www/checker/logs/%s' % (outputfilename)
                    outputfile = os.path.join(os.getcwd(), "logs", f'{outputfilename}')
                    isql = text(sql)
                    rsql = eng.execute(isql)
                    df = DataFrame(rsql.fetchall())
                    if len(df) > 0:
                        df.columns = rsql.keys()
                        df.columns = [x.lower() for x in df.columns]
                        df.to_csv(outputfile,header=True, index=False, encoding='utf-8')
                    else:
                        response = jsonify({'code': 200,'link': outlink})
                        #response.headers['Access-Control-Allow-Origin'] = '*'
                        return response

        export_link = outlink
        response = jsonify({'code': 200,'link': export_link})
        return response

@download.route('/logs', methods = ['GET'])
def log_file():
    print("log route")
    filename = request.args.get('filename')
    print(filename)

    if filename is not None:
        print(os.getcwd(), "logs", filename)
        # code below has been changed from attachment_file to download_name - paul 9jan23
        return send_file( os.path.join(os.getcwd(), "logs", filename), as_attachment = True, download_name = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "logs", filename)) \
            else jsonify(message = "file not found")
    else:
        return jsonify(message = "no filename was provided")

@download.route('/loggerdownload', methods = ['GET'])
def logger_download():

    data = pd.read_sql(
        """
            SELECT 
                MIN(samplecollectiontimestampstart::DATE) as start_ts, 
                MAX(samplecollectiontimestampend::DATE) AS end_ts 
            FROM 
                tbl_wq_logger_metadata;
        """,
        g.eng
    )
    start_ts = data.start_ts.iloc[0]
    end_ts = data.end_ts.iloc[0]

    return render_template("logger_download.html", start_ts=start_ts, end_ts=end_ts)

@download.route('/loggerdownload/populate-dropdown', methods = ['POST'])
def populate_dropdown():

    start_ts = request.json.get('logger_start')
    end_ts = request.json.get('logger_end')
    
    data = pd.read_sql(
        f"""
            SELECT 
                projectid, estuaryname, sensortype
            FROM 
                tbl_wqlogger
            WHERE samplecollectiontimestamp >= '{start_ts} 00:00:00'
            AND samplecollectiontimestamp <= '{end_ts} 23:59:59'
            GROUP BY projectid, estuaryname, sensortype
            ORDER BY projectid, estuaryname, sensortype;
            ;
        """,
        g.eng
    )
    projectid =  list(set(data.projectid))
    estuaryname = list(set(data.estuaryname))
    sensortype = list(set(data.sensortype))
    
    return jsonify(projectid=projectid, estuaryname=estuaryname, sensortype=sensortype)


@download.route('/loggerdownload/apidocs', methods = ['GET'])
def logger_download_template():
    swagger_json_path = url_for('static', filename='swagger.json')
    return render_template("swagger_ui.html",  swagger_json_path=swagger_json_path)

@download.route('/loggerdownload/repopulate-dropdown', methods = ['POST'])
def repopulate_dropdown():
    
    projectid = request.json.get('projectid')
    estuaryname = request.json.get('estuaryname')
    sensortype = request.json.get('sensortype')
    print(projectid, estuaryname, sensortype)

    sql = f"""
        SELECT 
            DISTINCT projectid, estuaryname, sensortype 
        FROM 
            tmp_logger_meta WHERE 
        """

    conditions = []

    if projectid is not None:
        conditions.append(f"projectid IN ({projectid})")
    if estuaryname is not None:
        conditions.append(f"estuaryname IN ({estuaryname})")
    if sensortype is not None:
        conditions.append(f"sensortype IN ({sensortype})")

    if conditions:
        sql += " AND ".join(conditions)
    else:
        sql = sql.rstrip(" WHERE ")
    print(sql)
    df = pd.read_sql(sql, g.eng)

    projectid = list(set(df['projectid']))
    estuaryname = list(set(df['estuaryname']))
    sensortype = list(set(df['sensortype']))

    return jsonify(projectid=projectid, estuaryname=estuaryname, sensortype=sensortype)

@download.route('/loggerdownload/getminmaxtimestamp', methods = ['POST'])
def get_minmax_ts():
    projectid = request.json.get('projectid')
    siteid = request.json.get('siteid')
    estuaryname = request.json.get('estuaryname')
    sensortype = request.json.get('sensortype')
    data = pd.read_sql(
        f"""
            SELECT 
                MIN(samplecollectiontimestamp) as min_ts,
                MAX(samplecollectiontimestamp) as max_ts
            FROM
                tbl_wqlogger
            WHERE
                projectid IN ({projectid}) 
                AND estuaryname IN ({estuaryname}) 
                AND sensortype IN ({sensortype})
        """,
        g.eng
    )
    min_ts = data.min_ts.iloc[0]
    max_ts = data.max_ts.iloc[0]

    return jsonify(min_ts=min_ts, max_ts=max_ts)


@download.route('/loggerdownload/checkavailability', methods = ['POST'])
def check_availability():
    logger_start = request.json.get('logger_start')
    logger_end = request.json.get('logger_end')

    data = pd.read_sql(
        f"""
            SELECT DISTINCT estuaryname, sensortype
            FROM
                tbl_wq_logger_metadata
            WHERE
                samplecollectiontimestampstart >= '{logger_start}'
                AND samplecollectiontimestampend <= '{logger_end}'
        """,
        g.eng
    )
    
    estuaryname = data['estuaryname'].tolist()
    sensortype = data['sensortype'].tolist()

    return jsonify(estuaryname=estuaryname, sensortype=sensortype)


@download.route('/getloggerdata', strict_slashes=False, methods = ['POST'])
def get_logger_data():
    import pandas as pd
    import re
    
    eng = g.eng
    payload = request.json

    # Prevent SQL Injection
    for k,v in payload.items():
        if k != 'is_partitioned':
            payload[k] = re.sub(r'[#;]', '', v)
    
    start_date = pd.Timestamp(payload.get('start_time'))
    end_date = pd.Timestamp(payload.get('end_time'))

    if any([start_date is None, end_date is None]):
        return jsonify(message="Start Date and End Date must be provided")

    projectid = payload.get('projectid')
    siteid = payload.get('siteid')
    estuaryname = payload.get('estuaryname')
    sensortype = payload.get('sensortype')

    colnames = [
        x 
        for x in  
        pd.read_sql(
            f"SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_name = '{payload.get('base_table')}'", g.eng
        ).column_name.tolist()
        if x not in current_app.system_fields
    ]
 

    # Initialize an empty list to store the tuples
    date_list = []

    # Set the initial date to the start date
    current_date = start_date

    # Iterate through each month between start and end date
    while current_date <= end_date:
        # Extract year and month from the current date
        year = current_date.year
        month = current_date.month
        
        # Append the year and month tuple to the list
        date_list.append((year, month))
        
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    
    if payload.get('is_partitioned'):

        combined_table_str = "("
        combined_table_str += " UNION ALL ".join(
            [
                """
                    SELECT 
                        {}
                    FROM 
                        {}_{} 
                    WHERE 
                        {} >= '{}' 
                        AND {} <= '{}'
                """.format(
                    ",".join(colnames),
                    payload.get('base_table'),
                    "m".join([str(yr_qt_tup[0]),str(yr_qt_tup[1])]),
                    payload.get('datetime_colname'),
                    payload.get('start_time'),
                    payload.get('datetime_colname'),
                    payload.get('end_time')
                )
                
                for yr_qt_tup in 
                    date_list
            ]
        )

        combined_table_str += ") AS t"
    
        sql = f"SELECT * FROM {combined_table_str} WHERE "
        
        conditions = []

        if projectid is not None:
            conditions.append(f"t.projectid IN ({projectid})")
        if estuaryname is not None:
            conditions.append(f"t.estuaryname IN ({estuaryname})")
        if sensortype is not None:
            conditions.append(f"t.sensortype IN ({sensortype})")

        if conditions:
            sql += " AND ".join(conditions)
        else:
            sql = sql.rstrip(" WHERE ")
        
        print("sql")
        print(sql)
        
        sessionid = int(time.time())
        # records = pd.read_sql(sql, eng).iloc[0,0]
        # df = pd.DataFrame(records)
        csv_path = f"/tmp/loggerdata_{sessionid}.csv"
        
        cmdlist = [
            'psql', 
            os.environ.get('DB_CONNECTION_STRING'),
            '-c', 
            f"\COPY ({sql}) TO \'{csv_path}\' CSV HEADER"
        ]
    
        # time the query
        query_begin_time = time.time()
        proc = sp.run(cmdlist, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines = True)
        if proc.returncode != 0:
            print(f"Error: {proc.stderr}")
        else:
            print("success")
        print(f"takes {time.time() - query_begin_time} seconds to run the query")
    
    blob = open(csv_path, 'rb').read()
    os.remove(csv_path)

    #return send_file(path, as_attachment = True, attachment_filename = filename)
    return send_file(
        BytesIO(blob), 
        download_name = 'logger.csv', 
        as_attachment = True, 
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


    #return jsonify(records)