import pandas as pd
from flask import request, Blueprint, render_template, current_app, g
from .utils.mail import send_mail


scraper = Blueprint('scraper', __name__)
@scraper.route('/scraper', methods=['GET'])
def lookuplists():
    print("start scraper")
    if request.args.get("action"):
        action = request.args.get("action")
        message = str(action)
        if request.args.get("layer"):
            layer = request.args.get("layer")
            datatype = request.args.get('datatype')

            # layer should start with lu - if not return empty - this tool is only for lookup lists
            if layer.startswith("lu_") or layer.startswith("xwalk_") or layer.endswith("_assignment") or layer.endswith("_completion_status"):

                # unfortunately readonly user doesnt have access to information_schema
                eng = g.eng # postgresql

                # below should be more sanitized
                # https://stackoverflow.com/questions/39196462/how-to-use-variable-for-sqlite-table-name?rq=1
                # check to make sure table exists before proceeding
                # get primary key for lookup list
                sql_primary = f"""
                    SELECT DISTINCT(kcu.column_name) 
                    FROM information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                    JOIN information_schema.constraint_column_usage AS ccu 
                    ON ccu.constraint_name = tc.constraint_name 
                    WHERE constraint_type = 'PRIMARY KEY' 
                    AND tc.table_name='{layer}';
                    """ 

                print(sql_primary)

                try:
                    primary_key_result = eng.execute(sql_primary)
                    # there should be only one primary key
                    primary_key = primary_key_result.fetchone()
                    print(f"primary_key: {primary_key}")
                    try:
                        # get all fields first
                        print("get all fields first")
                        
                        # for field and sample assignment tables, it is too cluttered. We should reduce fields that are displayed
                        fieldlist = ['stationid','latitude','longitude','stratum','region','parameter','assigned_agency']
                        if layer.endswith('sample_assignment'):
                            fieldlist.append('datatype')
                        
                        fields = ','.join(fieldlist) if layer.endswith('_assignment') else 'sampleid,matrix' if layer.endswith("chem_intercal_samples") else '*'

                        scrape_qry = f"SELECT {fields} FROM {layer}"
                        
                        if layer.endswith("sample_assignment") and (datatype is not None):
                            scrape_qry += f" WHERE UPPER(datatype) = '{str(datatype).upper()}'"

                        elif primary_key:
                            scrape_qry += f" ORDER BY {primary_key[0]} ASC;"
                            
                        
                        if layer.endswith("sample_assignment"):
                            scrape_qry += f" ORDER BY stationid, datatype, parameter;"
                        
                        scraper_results = pd.read_sql(scrape_qry, eng)
                        
                        #print(scraper_results)
                        # for bight we dont want system columns
                        for fieldname in current_app.system_fields:
                            if fieldname in scraper_results:

                                # 2nd arg is the "axis" argument, i assume
                                scraper_results = scraper_results.drop(fieldname, 1)
                        
                        # turn dataframe into dictionary object
                        scraper_json = scraper_results.to_dict('records')
                        # give jinga the listname, primary key (to highlight row), and fields/rows
                        return render_template('scraper.html', list=layer, primary=primary_key[0] if primary_key is not None else primary_key, scraper=scraper_json)
                    # if sql error just return empty 
                    except Exception as err:
                        print(err)
                        return "empty"
                except Exception as err:
                    print(err)
                    return "empty"
            else:
                return "empty"

    return "Nothing found"


# When an exception happens when the browser is sending requests to the scraper blueprint, this routine runs
@scraper.errorhandler(Exception)
def scraper_error_handler(error):
    send_mail(
        current_app.mail_from, 
        current_app.maintainers, 
        "Exception in fetching lookup lists - scraper.py", 
        str(error),
        server = current_app.config['MAIL_SERVER']
    )
    return "exception occurred trying to fetch lookup list"