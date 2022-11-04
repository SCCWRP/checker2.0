from pandas.io import excel
from flask import render_template, request, jsonify, current_app, Blueprint, session, g, send_file
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine, text, exc, Table, MetaData
import urllib, json
from pandas import DataFrame
from os import environ
from openpyxl import Workbook
from openpyxl.styles import Border, Side
from pandas import DataFrame
import re
import os
import pandas as pd
import numpy as np
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.utils.dataframe import dataframe_to_rows
from .core.functions import get_primary_key

# dynamic lookup lists to template

templater = Blueprint('templater', __name__)
@templater.route('/templater', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
# consider using the app.datasets dictionary to generalize the code better
def template():
    system_fields = current_app.system_fields
    datatype = request.args.get("datatype")
    eng = g.eng
    # intialize metadata
    # intialize metadata
    meta = MetaData()
    # get primary and foreign keys
    sql = eng.execute(
        """
        SELECT conrelid::regclass AS table_from, conname, pg_get_constraintdef(oid) 
        FROM pg_constraint WHERE contype IN ('f', 'p') AND connamespace = 'sde'::regnamespace AND conname LIKE 'tbl%%' 
        ORDER BY conrelid::regclass::text, contype DESC
        """
    )
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    # df: datframe of tables and their primary and foreign keys
    df = sql_df
    del sql_df

    df = df[df['table_from'].str[:4].str.contains('tbl_')]
    grouped_df = df.groupby('table_from')

    for key, item in grouped_df:
        print(grouped_df.get_group(key), "\n\n")

    descr_grouped_df = grouped_df.describe()
    print(descr_grouped_df)


    # This was hard-coded before. I didn't want to change it since it is already here, and it works. - Duy
    if datatype == 'logger':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_wq_logger_metadata',
            'tbl_logger_ctd_data',
            'tbl_logger_mdot_data',
            'tbl_logger_troll_data',
            'tbl_logger_tidbit_data'
        ]
        file_prefix = 'SOP_1_WQ_LOGGER'
    # SOP 2 Discrete WQ
    elif datatype == 'discretewq':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_waterquality_metadata',
            'tbl_waterquality_data'
        ]
        file_prefix = 'SOP_2_DISCRETE_WQ'
    # SOP 3 Nutrients
    elif datatype == 'nutrients_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_nutrients_metadata'
        ]
        file_prefix = 'SOP_3_NUTRIENTS_FIELD'
    elif datatype == 'nutrients_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_nutrients_labbatch_data',
            'tbl_nutrients_data'
        ]
        file_prefix = 'SOP_3_NUTRIENTS_LAB'
    # SOP 4 eDNA
    elif datatype == 'edna_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_edna_metadata'
        ]
        file_prefix = 'SOP_4_EDNA_FIELD'
    elif datatype == 'edna_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_edna_water_labbatch_data',
            'tbl_edna_sed_labbatch_data',
            'tbl_edna_data'
        ]
        file_prefix = 'SOP_4_EDNA_LAB'
    # SOP 5 Sediment Grain Size
    elif datatype == 'sedimentgrainsize_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sedgrainsize_metadata'
        ]
        file_prefix = 'SOP_5_SEDIMENTGRAINSIZE_FIELD'
    elif datatype == 'sedimentgrainsize_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sedgrainsize_labbatch_data',
            'tbl_sedgrainsize_data'
        ]
        file_prefix = 'SOP_5_SEDIMENTGRAINSIZE_LAB'
    # SOP 6 Benthic Infauna
    elif datatype == 'benthicinfauna_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_benthicinfauna_metadata'
        ]
        file_prefix = 'SOP_6_BENTHICINFAUNA_FIELD'
    elif datatype == 'benthicinfauna_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_benthicinfauna_labbatch',
            'tbl_benthicinfauna_abundance',
            'tbl_benthicinfauna_biomass'
        ]
        file_prefix = 'SOP_6_BENTHICINFAUNA_LAB'
    # SOP 7 Macroalgae
    elif datatype == 'macroalgae':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_macroalgae_sample_metadata',
            'tbl_algaecover_data',
            'tbl_floating_data'
        ]
        file_prefix = 'SOP_7_MACROALGAE'
    # SOP 7 SAV
    elif datatype == 'sav':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sav_metadata',
            'tbl_savpercentcover_data'
        ]
        file_prefix = 'SOP_7_SAV'
    # SOP 8 BRUV
    elif datatype == 'bruv_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_bruv_metadata',
        ]
        file_prefix = 'SOP_8_BRUV_FIELD'
    elif datatype == 'bruv_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_bruv_data',
            'tbl_bruv_videolog'
        ]
        file_prefix = 'SOP_8_BRUV_LAB'
    # SOP 9 Fish Seines
    elif datatype == 'fishseines':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_fish_sample_metadata',
            'tbl_fish_abundance_data',
            'tbl_fish_length_data'
        ]
        file_prefix = 'SOP_9_FISHSEINES'
    # SOP 10 Crab Trap
    elif datatype == 'crabtrap':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_crabtrap_metadata',
            'tbl_crabfishinvert_abundance',
            'tbl_crabbiomass_length'
        ]
        file_prefix = 'SOP_10_CRABTRAP'
    # SOP 11 Vegetation
    elif datatype == 'vegetation':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_vegetation_sample_metadata',
            'tbl_vegetativecover_data',
            'tbl_epifauna_data'
        ]
        file_prefix = 'SOP_11_VEGETATION'
    # SOP 13 Feldspar
    elif datatype == 'feldspar':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_feldspar_metadata',
            'tbl_feldspar_data'
        ]
        file_prefix = 'SOP_13_FELDSPAR'
    else:
        tbls = []

    # initialize list for primary and foreign keys extracted from database
    keys_from_db = []

    for key, item in grouped_df:
        if key in tbls:
            print(key)
            print(item['pg_get_constraintdef'])
            keys_from_db.append(item['pg_get_constraintdef'])

    list_of_keys = []
    list_of_lu_needed = []

    for fkey in keys_from_db:
        print(f"fkey ----- {fkey}")

        for lu in fkey:
            print(f"lu --- {lu}")
            print("appending list of keys: ")
            list_of_keys.append(lu)
    print("LIST OF KEY")
    print(list_of_keys)

    primarykeylist = list()
    tmplist = list()

    for line in list_of_keys:
    
        if "PRIMARY" in line:
            print(line)
            tmpprimary = line.split()[2:]
            for primary in tmpprimary:
                primary = re.sub('\(|\)|\,|', '', primary)
                primarykeylist.append(primary)
        for element in line.split():
            if element.startswith('lu'):
            
                list_of_lu_needed.append(element.split('(')[0])
            if element.startswith('('):
                foreignkey = element.strip('()')
                tmplist.append(foreignkey)

    print("THIS IS FOREIGN KEY COLUMNS(TBLS) + PRIMARY KEY COLUMNS(LOOK_UP) LIST ")
    print(tmplist)
    # Currently at this point, the tmplist has only foreign key columns for the tabs/tbls. This list will be updated with the primary key columns(look-up list) later.
    print("THIS IS PRIMARY KEY COLUMN(TBLS) LIST")
    print(primarykeylist)
    list_lu_needed = list(set(list_of_lu_needed))
    print(list_of_lu_needed)
    print("Building Templates and Adding all Lu list")
    glossary = pd.DataFrame()
    for tbl in tbls:
        df = pd.read_sql(
            f"""
            SELECT
                cols.column_name as field_name,
                    cols.data_type as field_type,
                    (
                    SELECT
                        pg_catalog.col_description(c.oid, cols.ordinal_position::int)
                    FROM
                        pg_catalog.pg_class c
                    WHERE
                        c.oid = (SELECT ('"' || cols.table_name || '"')::regclass::oid)
                        AND c.relname = cols.table_name
                ) AS description
            FROM
                information_schema.columns cols
            WHERE
                cols.table_catalog    = 'empa2021'
                AND cols.column_name NOT IN ({','.join([f"'{x}'" for x in system_fields])})
                AND cols.table_name   = '{tbl}';
            
            """,
            eng
        )
        df = df.assign(
            sheet = pd.Series([tbl.replace("tbl_","") for _ in range(len(df))]),
            template_prefix = pd.Series([f"{file_prefix}-TEMPLATE" for _ in range(len(df))])
        )
        df = df[['template_prefix','sheet', 'field_name', 'field_type','description' ]]
        glossary = pd.concat([glossary, df],ignore_index=True)
   

    xls = {
        **{
            'Instructions': pd.DataFrame(
                
                    {
                        'How to use:': [
                            "Information about this spreadsheet:",
                            "SCCWRP spreadsheets follow a standard format, each consisting of several sheets: protocol metadata, sample metadata, sample data, and a glossary.",
                            "Metadata for each column can be found by selecting the header cell for that column, or in the glossary sheet. Please do not add or rename columns. Use note columns to provide additional information or context.",
                            "Questions or comments? Please contact Jan Walker at janw@sccwrp.org or Liesl Tiefenthaler at lieslt@sccwrp.org"
                        ]
                    }
                
            )
        },
        **{
            table.replace("tbl_", ""): pd.DataFrame(
                columns=
                [
                    *['projectid'],
                    *[
                        x for x in pd.read_sql(
                            f"""
                                SELECT *  FROM {table} LIMIT 1
                            """,
                            eng
                        ).columns.to_list()
                        if x not in system_fields and x != 'projectid'
                    ]
                ]
            ) for table in tbls
        },
        **{
            'glossary': glossary
        },
        **{
            lu_name: pd.read_sql(f"SELECT * from {lu_name}", eng).drop(columns=['objectid'])
            for lu_name in list(set(list_lu_needed))
        }
    }

    print("Re-ordering columns")
    column_order = pd.read_sql("SELECT * from column_order", eng)
    column_order = dict(zip(column_order['table_name'],column_order['column_order']))
    for table in [f"tbl_{x}" for x in xls.keys() if f"tbl_{x}" in column_order.keys()]:
        try:
            print(table)
            correct_field_order = column_order[table].split(",")
            xls[table.replace("tbl_","")].columns = [
                *[x for x in correct_field_order if x in xls[table.replace("tbl_","")].columns],
                *[x for x in xls[table.replace("tbl_","")].columns if x not in correct_field_order]
            ]
        except Exception as e:
            print(e)
        
    print("Done reordering columns")
    ############################################################################################################################
    ### Legacy code. I wrote them when I first started SCCWRP and worked on this project. They are not optimized, but still work.
    ### I will improve the code when I have time - Duy 10/11/22
     ############################################################################################################################
    print("Finished building templates and adding all Lu list")
    lookup_list = pd.read_sql(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'lu%%'", 
        eng
    )
    lookup_list = lookup_list['table_name'].tolist()

    # gets lookup lists and their primary key columns so we can highlight
    # in actuality this query checks the foreign key constraint to grab which column to highlight
    def getlookupcols(tbls):
        qry = f"""
            SELECT DISTINCT
                ccu.TABLE_NAME AS table,
                ccu.COLUMN_NAME AS primarycolumn_0 
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
                AND ccu.table_schema = tc.table_schema 
            WHERE
                tc.constraint_type = 'FOREIGN KEY' 
                AND tc.TABLE_NAME IN ('{"','".join(tbls)}')
                AND ccu.TABLE_NAME LIKE'lu_%%';
        """                
        return pd.read_sql(qry,eng)

        
    # pkeytable is based on the old way we used to query for the foreign key columns.
    # by looking at the primary key column of the lu table
    print("find primary function")
    pkeytable = getlookupcols(list_lu_needed)
    print("end find primary function")
    print("-----------------------------")
    print(" Printing pkey table below...")
    print("-----------------------------")
    print(pkeytable)


    def highlight_primary_key_column(dict):
        columns_to_highlight = list()
        for i in dict.keys():
            for index, row in pkeytable.iterrows():
                if i == row['table']:
                    df = dict[i]
                    for ii in df.columns:
                        if ii == row['primarycolumn_0']:
                            columns_to_highlight.append(ii)
        print(columns_to_highlight)
        return columns_to_highlight


    # RETRIEVE LOOKUP LISTS FROM THE DATABASE
    ###################
    columnlist = list()
    columndict = dict()
    for i, lu_list in enumerate(list_of_lu_needed):
        # It's easier to use pd.read_sql in my opinion
        sql_df = pd.read_sql("SELECT * FROM " + lu_list, eng)
        print("retrieved sql_df for lookups...")
        # sql_df = sql_df[[x for x in sql_df.columns if x not in app.system_fields]] # no app.system_fields here
        sql_df = sql_df[[x for x in sql_df.columns if x not in system_fields]]
        templater = dict({lu_list: sql_df})
        print("BEGIN HIGHLIGHT PRIMARY KEY COLUMN FUNCTION")
        columns_to_highlight = highlight_primary_key_column(templater)
        print("END HIGHLIGHT PRIMARY KEY COLUMN FUNCTION")
        # columnlist is the primary keys for the lu lists
        columnlist.extend(columns_to_highlight)
        for value in columns_to_highlight:
            test = dict()
            test[columns_to_highlight[0]] = sql_df[value]
            columndict.update(test)
    columnlist.extend(tmplist)

    print("exporting to original_df to excel file")

    print("==================================================")
    print("==================================================")
    print("==================================================")

    excel_file = f"{os.getcwd()}/export/routine/{file_prefix}-TEMPLATE.xlsx"
    excel_writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')


    for i, sheet in enumerate(xls.keys()):

        xls[sheet].to_excel(excel_writer, sheet_name=sheet,
                                startrow=1, index=False, header=False)
        workbook = excel_writer.book
        worksheet = excel_writer.sheets[sheet]
        # bold indicated foreign keys, otherwise not bold
        # format 1 is for FOREIGN KEY COLUMNS
        format1 = workbook.add_format(
            {'bold': False, 'text_wrap': True, 'fg_color': '#D7D6D6'})
        format1.set_align('center')
        format1.set_align('vcenter')
        format1.set_rotation(90)
        # format 2 is for REGULAR COLUMNS
        format2 = workbook.add_format({'bold': False, 'text_wrap': True})
        format2.set_align('center')
        format2.set_align('vcenter')
        format2.set_rotation(90)
        # format 3 is for PRIMARY KEY COLUMNS
        format3 = workbook.add_format({'bold': True, 'text_wrap': True})
        format3.set_align('center')
        format3.set_align('vcenter')
        format3.set_rotation(90)
        # format 4 is for PRIMARY KEY AND ALSO FOREIGN KEY COLUMNS
        format4 = workbook.add_format(
            {'bold': True, 'text_wrap': True, 'fg_color': '#D7D6D6'})
        format4.set_align('center')
        format4.set_align('vcenter')
        format4.set_rotation(90)
        if sheet == 'Instructions':
            print(f"sheet: {sheet}")
            continue
        for col_num, col_name in enumerate(xls[sheet].columns.values):
            if (col_name.lower() in primarykeylist) & (col_name.lower() not in columnlist):
                worksheet.write(0, col_num, col_name, format3)
                worksheet.set_row(0, 170)
            elif (col_name.lower() in primarykeylist) & (col_name.lower() in columnlist):
                worksheet.write(0, col_num, col_name, format4)
                worksheet.set_row(0, 170)
            elif (col_name.lower() in columnlist) & (col_name.lower() not in primarykeylist):
                worksheet.write(0, col_num, col_name, format1)
                worksheet.set_row(0, 170)
            else:
                worksheet.write(0, col_num, col_name, format2)
                worksheet.set_row(0, 170)
    del i, sheet
    excel_writer.save()


    grouped_df = glossary.groupby(['sheet'])
    gb_df = grouped_df.groups
    key_df = gb_df.keys()

    wb = openpyxl.load_workbook(excel_file)

    for key, values in gb_df.items():
        sh = wb[key.lower()]
        tmp = glossary[glossary['sheet'] == key]
        field_df_dict = dict(zip(tmp['field_name'].apply(lambda x: x.lower()), tmp['description']))
        n = len(tmp.field_name.tolist())
        for row in sh.iter_rows(min_row=1, min_col=1, max_row=1, max_col=n):
            for cell in row:
                
                dv = DataValidation()
                s = str(field_df_dict.get(cell.value,None)) 
                dv.prompt = s
                sh.add_data_validation(dv)
               
                dv.add(cell)
                print("data validation description added to cell")

    wb.save(excel_file)
    wb.close()
    ############################################################################################################################
    ############################################################################################################################
    return send_file(f"{os.getcwd()}/export/routine/{file_prefix}-TEMPLATE.xlsx", as_attachment=True, attachment_filename=f'{file_prefix}-TEMPLATE.xlsx')
    




