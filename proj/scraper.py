import pandas as pd
from flask import request, Blueprint, render_template, current_app, g
from .utils.mail import send_mail


scraper = Blueprint('scraper', __name__)
@scraper.route('/lookuplists', methods=['GET'])
@scraper.route('/scraper', methods=['GET'])
def lookuplists():
    print("start scraper")

    eng = g.readonly_eng # postgresql - readonly user

    if request.args.get("action"):
        action = request.args.get("action")
        message = str(action)
        if request.args.get("layer"):
            layer = request.args.get("layer")
            datatype = request.args.get('datatype')

            # layer should start with lu - if not return empty - this tool is only for lookup lists
            if layer.startswith("lu_"):

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
                        
                        # get all the data from the lookup list
                        scrape_qry = f"SELECT * FROM {layer}"
                        
                        scrape_qry += f" ORDER BY {primary_key[0]} ASC;" if primary_key else ";"
                        
                        scraper_results = pd.read_sql(scrape_qry, eng)
                        
                        # bight we dont want system columns
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

    
    all_lookups = pd.read_sql("""SELECT "table_name" FROM information_schema.tables WHERE "table_name" LIKE 'lu_%%' ORDER BY "table_name";""", eng).table_name.tolist()
    return render_template('scraper.html', list_all = True, all_lookups = all_lookups)




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