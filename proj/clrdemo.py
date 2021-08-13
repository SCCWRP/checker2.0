from flask import Blueprint, g
from sqlalchemy import create_engine

clrdemo = Blueprint('clrdemo', __name__)
@clrdemo.route('/clrdemo')
def flushdemo():
    tables = ['testsite', 'watershed', 'bmpinfo', 'monitoringstation', 'precipitation', 'ceden_waterquality','flow']
    sql = f"DELETE FROM mobile_testsite WHERE sitename = 'Albany Park and Ride NZ'; DELETE FROM unified_testsite WHERE sitename = 'Albany Park and Ride NZ'; "
    sql += f"DELETE FROM unified_precipitation WHERE sitename = 'Albany Park and Ride NZ'; "
    sql += '; '.join([f"DELETE FROM tbl_{x} WHERE sitename = 'Albany Park and Ride NZ'" for x in tables])
    g.eng.execute(sql)
    return 'demo data cleared'