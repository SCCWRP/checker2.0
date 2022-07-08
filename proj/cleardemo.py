import pandas as pd
from flask import jsonify, Blueprint, g, current_app

clear_test_data = Blueprint('clear_test_data', __name__)
@clear_test_data.route('/cleartestdata', methods = ['GET','POST'])
def clear_test():
    eng = g.eng
    tbls = pd.read_sql("SELECT table_name FROM information_schema.columns WHERE table_name LIKE 'tbl_%%' AND column_name = 'login_email';", eng).table_name.values
    sql = ';\n'.join([f"DELETE FROM {tbl} WHERE login_email = 'test@sccwrp.org'" for tbl in tbls])
    eng.execute(sql)
    return jsonify(message=f"test data cleared from {current_app.project_name} database")