import os, json
from flask import send_file, Blueprint, jsonify, request, g, current_app
from flask_cors import CORS, cross_origin
from pandas import read_sql, DataFrame
from sqlalchemy import text
from zipfile import ZipFile
import time, datetime
import functools
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
cors = CORS(download, resources={r"/exportdata/*": {"origins": "*"}})

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
@cross_origin()
def exportdata():
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
        valid_tables = {'algaecover': 'tbl_algaecover_data','benthicmetadata': 'tbl_benthicinfauna_metadata','bruvmetadata': 'tbl_bruv_metadata','crabbiomass': 'tbl_crabbiomass_length','crababundance': 'tbl_crabfishinvert_abundance','fishabundance': 'tbl_fish_abundance_data', 'fishlength':'tbl_fish_length_data'}
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
                    if table_name in valid_tables and check == 1:
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
                        return response

        export_link = outlink
        #admin_engine.dispose()
        #query_engine.dispose()
        response = jsonify({'code': 200,'link': export_link})
        return response

@download.route('/logs', methods = ['GET'])
def log_file():
    print("log route")
    filename = request.args.get('filename')
    print(filename)

    if filename is not None:
        print(os.getcwd(), "logs", filename)
        return send_file( os.path.join(os.getcwd(), "logs", filename), as_attachment = True, attachment_filename = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "logs", filename)) \
            else jsonify(message = "file not found")
    else:
        return jsonify(message = "no filename was provided")
