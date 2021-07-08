from flask import Blueprint, current_app, session
from .utils.db import GeoDBDataFrame

import pandas as pd

finalsubmit = Blueprint('finalsubmit', __name__)
@finalsubmit.route('/load', methods = ['GET','POST'])
def load():
    excel_path = session['excel_path']

    eng = current_app.eng

    all_dfs = {

        # Some projects may have descriptions in the first row, which are not the column headers
        # This is the only reason why the skiprows argument is used.
        # For projects that do not set up their data templates in this way, that arg should be removed

        # Note also that only empty cells will be regarded as missing values
        sheet: pd.read_excel(excel_path, sheet_name = sheet, skiprows = current_app.excel_offset, na_values = [''])
        
        for sheet in pd.ExcelFile(excel_path).sheet_names
        
        if sheet not in current_app.tabs_to_ignore
    }

    valid_tables = pd.read_sql(
            # percent signs are escaped by doubling them, not with a backslash
            # percent signs need to be escaped because otherwise the python interpreter will think you are trying to create a format string
            "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tbl_%%'", eng
        ) \
        .table_name \
        .values

    assert all(sheet in valid_tables for sheet in all_dfs.keys()), \
        f"Sheetname in excel file {excel_path} not found in the list of tables that can be submitted to"
    


