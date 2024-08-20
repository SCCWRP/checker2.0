import os
import time
import pandas as pd
import json
import sqlite3

from io import BytesIO
from copy import deepcopy
from flask import send_file, Blueprint, jsonify, request, g, current_app, render_template, send_from_directory, make_response
from pandas import read_sql, DataFrame
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import PatternFill

from .utils.excel import format_existing_excel

report_bp = Blueprint('report', __name__)

# This view is pretty specific to the bight project
# I am considering making a folder called custom blueprints
@report_bp.route('/report', methods=['GET', 'POST'])
def report():
    valid_datatypes = ['field', 'chemistry', 'infauna', 'toxicity', 'microplastics']
    
    # Give option for them to download specific data report views - which Dario requested in April 2024
    # As we create more views, we can add more here
    valid_views = ['vw_trawl_completion_status_simplified', 'vw_grab_completion_status_simplified']
    specificview = request.args.get('view')
    
    if specificview is not None:
        if specificview not in valid_views:
            return jsonify({"error":"Bad Request", "message": f"{specificview} is not a valid name for a view"}), 400
        
        excel_blob = BytesIO()
        data = pd.read_sql(f"SELECT * FROM {specificview}", g.eng)
        with pd.ExcelWriter(excel_blob) as writer:
            data.to_excel(writer, sheet_name = specificview[:31], index = False)
        excel_blob.seek(0)
        
        # apply formatting
        print("# apply formatting")
        formatted_blob = format_existing_excel(excel_blob)
        
        # Make a response object to set a custom cookie
        resp = make_response(send_file(formatted_blob, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', download_name=f"""{specificview}.xlsx"""))

        return resp


    datatype = request.args.get('datatype')

    if datatype is None:
        print("No datatype specified")
        return render_template(
            'report.html',
            datatype=datatype
        )
    if datatype in valid_datatypes:
        report_df = pd.read_sql(f'SELECT * FROM vw_tac_{datatype}_completion_status', g.eng)
        
        # Write report_df to export folder so we can download it
        report_df_for_download = deepcopy(report_df)
        report_df_for_download = report_df_for_download.assign(
            stations = report_df_for_download.stations.apply(lambda x: [x.strip() for x in x.split(",")])    
        )
        print(report_df_for_download)
        report_df_for_download = report_df_for_download.explode('stations')
        report_df_for_download.to_excel(os.path.join(os.getcwd(), "export", f"report-{datatype}.xlsx"), index=False)
        #####

        report_df.set_index(
            ['submissionstatus', 'lab', 'parameter'], inplace=True)
    else:
        report_df = pd.DataFrame(
            columns=['submissionstatus', 'lab', 'parameter', 'stations'])
        report_df.set_index(['submissionstatus', 'lab'], inplace=True)



    return render_template(
        'report.html',
        datatype=datatype,
        tables=[report_df.to_html(
            classes=['w3-table', 'w3-bordered'], header="true", justify='left', sparsify=True)],
        report_title=f'{datatype.capitalize()} Completeness Report'
    )


# We need to put the warnings report code here
# Logic is to have a page that displays datatypes and allows them to select a datatype
# after selecting a datatype, they should be able to select a table that is associated with that datatype
# All this information is in the proj/config folder
#
# after selecting a table, it should display all warnings from that table
# (each table has a column called warnings, it is a varchar field and the string of warnings is formatted a certain way)
# example:
#  columnname - errormessage; columnname - errormessage2; columnname - errormessage3
# Would it have been better if we would have made it a json field? probably, but there must be some reason why we did it like this
#
# so when they select the table, we need to get all the warnings associated with that table,
#  selecting from that table where warnings IS NOT NULL
# Then we have to pick apart the warnings column text, gather all unique warnings and display to the user
# We need them to have the ability to select warnings, and then download all records from that table which have that warning

# a suggestion might be to do this how nick did the above, where he requires a query string arg,
# except we should put logic such that if no datatype arg is provided, we return a template that has links with the datatype query string arg

# example
#  <a href="/warnings-report?datatype=chemistry">Chemistry</a>
#  <a href="/warnings-report?datatype=toxicity">Toxicity</a>
# etc

@report_bp.route('/warnings-report',  methods=['GET', 'POST'])
def warnings_report():

    if request.method == 'POST':
        req = request.get_json()
        datatype = req['datatype']
        table = req['table']

        if datatype is None:
            return jsonify({'no datatype'})
        elif table is None:
            json_file = open('proj/config/config.json')
            data = json.load(json_file)
            dataset_options = data["DATASETS"].keys()
            table_options = data['DATASETS'][datatype]['tables']
            return jsonify({'tables': table_options})

    datatype = request.args.get('datatype')
    print(datatype)
    table = request.args.get('table')
    print(table)
    if datatype is None and table is None:
        json_file = open('proj/config/config.json')
        data = json.load(json_file)
        dataset_options = data["DATASETS"].keys()
        print("Missing fields")
        return render_template(
            'warnings-report.html',
            dataset_options=dataset_options
        )
    elif table is None:
        json_file = open('proj/config/config.json')
        data = json.load(json_file)
        dataset_options = data["DATASETS"].keys()
        table_options = data['DATASETS'][datatype]['tables']
        print("Missing fields")
        return render_template(
            'warnings-report.html',
            dataset_options=dataset_options,
            table_options=table_options
        )
    else:
        eng = g.eng
        sql_query = f"SELECT * FROM {table} WHERE warnings IS NOT NULL"
        tmp = pd.read_sql(sql_query, eng)
        warnings_array = tmp.warnings.apply(
            lambda x: [s.split(' - ', 1)[-1] for s in x.split(';')]).values
        unique_warnings = pd.Series(
            [item for sublist in warnings_array for item in sublist]).unique()
        df = pd.DataFrame(unique_warnings, columns=["Warnings"])
        print(df)

        warnings = df['Warnings'].tolist()

        return render_template(
            'warnings-report.html',
            datatype=datatype,
            table=table,
            warnings=warnings,
        )


@report_bp.route('/warnings-report/export/', methods=['GET', 'POST'])
def download():
    if request.method == 'POST':
        req = request.get_json()
        selected_warnings = req['warnings']
        table = req['table']

        columns = set()

        for warning in selected_warnings:
            columns_list = warning.split(' --- ')[0].split(', ')
            for column in columns_list:
                column = column.replace(' ', '')
                columns.add(column)

        print(f'columns: {columns}')


        export_name = f'{table}-export.xlsx'
        export_file = os.path.join(os.getcwd(), "export", "warnings_report", export_name)
        export_writer = pd.ExcelWriter(export_file, engine='xlsxwriter')

        warnings_data_dict = {}
        mismatched_rows = pd.DataFrame()

        for warning in selected_warnings:
            sql = f"SELECT * FROM {table} WHERE warnings LIKE %(warning)s"
            data = pd.read_sql(sql, g.eng, params={'warning': f"%{warning}%"})
            
            split_warnings = data['warnings'].str.split(';').apply(pd.Series, 1).stack()
            split_warnings.index = split_warnings.index.droplevel(-1)
            split_warnings.name = 'warning'

            del data['warnings']
            data = data.join(split_warnings)

            matching_rows = data[data['warning'] == warning]
            mismatched_rows = pd.concat([mismatched_rows, data[~data.index.isin(matching_rows.index)]])
            warnings_data_dict[warning] = matching_rows

        for index, row in mismatched_rows.iterrows():
            warning = row['warning']
            if warning in warnings_data_dict:
                warnings_data_dict[warning] = pd.concat([warnings_data_dict[warning], pd.DataFrame([row])])

        yellow_fill = PatternFill(
            start_color='00FFFF00',
            end_color='00FFFF00',
            fill_type='solid'
        )

        with export_writer:
            for index, warning in enumerate(warnings_data_dict):
                sheetname = f"error{index+1}"
                warnings_data_dict[warning].to_excel(export_writer, sheet_name = sheetname, index = False)


        for index, warning in enumerate(warnings_data_dict):
            
            sheetname = f"error{index+1}"
            
            wb = load_workbook(export_file)
            
            columns_list = warning.split(' --- ')[0].split(',')
            
            for column in columns_list:
                column = column.replace("'", '')
                column = column.replace(' ', '').lower()
            
                target_column = None
                target_header = column

                for col_idx, col in enumerate(wb[sheetname].iter_cols(max_row=1)):
                    if col[0].value == target_header:
                        target_column = col_idx + 1
                        break
                
                if target_column is not None:
                    for row in wb[sheetname].iter_rows(min_col=target_column, max_col=target_column):
                        for cell in row:
                            cell.fill = yellow_fill
                            warning_msg = warning.split(' --- ')[1]
                            cell.comment = Comment(warning_msg, "Checker")
                    wb.save(export_file)

        return send_from_directory(os.path.join(os.getcwd(), "export", "warnings_report"), export_name, as_attachment=True)
    


@report_bp.route('/completeness-report', methods=['GET', 'POST'])
def completeness_report():

    # The sqlalchemy database connection object
    eng = g.eng 

    # Types of reports and their corresponding views
    report_types = {
        'toxicity': {
            'stratum': 'vw_tox_stratum_completeness_report',
            'agency' : 'vw_tox_agency_completeness_report'
        },
        'chemistry': {
            'stratum': 'vw_chem_stratum_completeness_report',
            'agency' : 'vw_chem_agency_completeness_report'
        },
        'benthic': {
            'stratum': 'vw_benthic_infauna_stratum_completeness_report',
            'agency' : 'vw_benthic_infauna_agency_completeness_report'
        },
        'trawl': {
            'stratum': 'vw_trawl_data_stratum_completeness_report',
            'agency' : 'vw_trawl_data_agency_completeness_report'
        },
        'microplastics': {
            'stratum': 'vw_microplastics_stratum_completeness_report',
            'agency' : 'vw_microplastics_agency_completeness_report'
        },
    }

    report_type = request.args.get('report_type')


    if report_type is None:
        return render_template('completeness_report.jinja2', report_types = report_types.keys())
    
    
    # Prepare a BytesIO object to write the report to
    output = BytesIO()

    if report_type.lower() == 'all':
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            for type_key, views in report_types.items():
                for view_key, view_name in views.items():
                    print(view_name)
                    df = pd.read_sql(f"SELECT * FROM {view_name}", eng)
                    df.to_excel(writer, sheet_name=f"{type_key}_{view_key}", index = False)
            writer.save()

        output.seek(0)  # Important: move back to the start of the BytesIO object
        
        # Format the excel file
        output = format_existing_excel(output)

        return send_file(output, as_attachment=True, download_name="completeness_reports.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    
    # If they made no request for "All" datatypes, then make sure they arent putting something funny - it should be one of the specific ones
    if report_type not in report_types.keys():
        return "Bad request", 400


    # Generate specific report
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        view_names = report_types[report_type]
        for key, view in view_names.items():
            print(view)
            df = pd.read_sql(f"SELECT * FROM {view}", eng)
            df.to_excel(writer, sheet_name=f"{key}", index = False)
        writer.save()

    output.seek(0)  # Reset the buffer position to the beginning

    # Format the excel file
    output = format_existing_excel(output)

    return send_file(output, as_attachment=True, download_name=f'{report_type}_completeness_report.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')