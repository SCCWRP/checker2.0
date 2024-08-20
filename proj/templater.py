# Templater routine dynamically calls lookup lists from database to populate into the data templates.
# The updated templates will be outputted to the user with all tables and the lookup lists. 
#below is from microplastics
#from flask import send_from_directory, render_template, request, redirect, Response, jsonify, send_file, json, current_app
#below is from empa main.py
from flask import request, current_app, Blueprint, g, send_file, make_response, flash, render_template
from sqlalchemy import Table, MetaData
import pandas as pd
from pandas import DataFrame
import re
import os
import openpyxl
from openpyxl.worksheet.datavalidation import DataValidation
# dynamic lookup lists to template
# skip the formatting

templater = Blueprint('templater', __name__)

@templater.route('/templates', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
@templater.route('/templater', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
# consider using the app.datasets dictionary to generalize the code better
def template():
    system_fields = current_app.system_fields
    datatype = request.args.get("datatype")

    if datatype not in current_app.datasets.keys():
        if datatype is not None:
            flash(f"Datatype {datatype} not found")
        return render_template(
            "templates.jinja2",
            datasets = current_app.datasets,
            project_name = current_app.project_name,
            background_image = current_app.config.get("BACKGROUND_IMAGE")
        )
    
    tbls = current_app.datasets.get(datatype)['tables']
    file_prefix = datatype.upper()
    database_name = str(g.eng).replace(")","").split("/")[-1]
    print(current_app.datasets.keys())

    static_template = current_app.datasets.get(datatype).get('template_filename')
    print("datatype")
    print(datatype)
    print("static_template")
    print(static_template)
    if static_template is not None:
        print("Static template")
        return send_file(f"{os.getcwd()}/export/data_templates/{static_template}", as_attachment=True, download_name=f'{static_template}')
    
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
                cols.table_catalog    = '{database_name}'
                AND cols.column_name NOT IN ({','.join([f"'{x}'" for x in system_fields])})
                AND cols.table_name   = '{tbl}';
            
            """,
            eng
        )
        df = df.assign(
            sheet = pd.Series([tbl.replace("tbl_","").replace("microplastics_","") for _ in range(len(df))]),
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
                            "SCCWRP spreadsheets follow a standard format, each consisting of several sheets: data templates, lookup lists, and a glossary.",
                            "Metadata for each column can be found by selecting the header cell for that column, or in the glossary sheet. Please do not add or rename columns. Use note columns to provide additional information or context.",
                            "Questions or comments? Please contact Paul Smith at pauls@sccwrp.org"
                        ]
                    }
                
            )
        },
        **{
            table.replace("tbl_", "").replace("microplastics_",""): pd.DataFrame(
                columns=
                [
                    *[
                        x for x in pd.read_sql(
                            """
                                SELECT {} FROM {} LIMIT 1
                            """.format(
                                ','.join(pd.read_sql(f"SELECT column_name FROM column_order WHERE table_name = '{table}' ORDER BY custom_column_position;", eng).column_name.tolist()),
                                table
                            ),
                            eng
                        ).columns.to_list()
                        if x not in system_fields
                    ]
                ]
            ) for table in tbls
        },
        **{
            'glossary': glossary
        },
        **{
            lu_name: pd.read_sql(f"SELECT * from {lu_name}", eng).drop(columns=['objectid'], errors = 'ignore')
            for lu_name in list(set(list_lu_needed))
        }
    }

    # Reorder columns of tbls
    # print("Re-ordering columns")
    # column_order = pd.read_sql("SELECT * from column_order", eng)
    # column_order = dict(zip(column_order['table_name'],column_order['column_order']))
    # for key in xls.keys():
    #     tab_name = f"tbl_{key}"
    #     if tab_name in tbls:
    #         df = xls[key]
    #         print("Before reordering:", df.columns, sep="\n")
    #         print(tab_name)
    #         correct_field_order = column_order.get(tab_name, None).split(",")
    #         print("correct_field_order",correct_field_order,sep="\n")
    #         if correct_field_order is not None:
    #             df = df[[x for x in correct_field_order if x in df.columns] + [x for x in df.columns if x not in correct_field_order]]
    #             xls[key] = df
    #             print("After reordering:", df.columns, sep="\n")

    # print("Done reordering columns")
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

    primarykey = dict()

    def findprimary(x):
        for i in lookup_list:
            s = Table(i, meta, autoload=True, autoload_with=eng)
            lpkeys = list()
            for pk in s.primary_key:
                pri = pk.name
                lpkeys.append(pri)
                primarykey[i] = lpkeys
        pkeytable = pd.DataFrame.from_dict(primarykey, orient='index')
        pkeytable.reset_index(level=0, inplace=True)
        for i in pkeytable.columns:
            if i == 'index':
                pkeytable.rename(columns={i: "table"}, inplace=True)
            else:
                pkeytable.rename(
                    columns={i: "primarycolumn_" + str(i)}, inplace=True)
        pkeytable = pkeytable.sort_values('table')
        return pkeytable


    pkeytable = findprimary(lookup_list)
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
        columns_to_highlight = highlight_primary_key_column(templater)
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

    excel_file = f"{os.getcwd()}/export/data_templates/{file_prefix}-TEMPLATE.xlsx"
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
    # wb.close()
    ############################################################################################################################
    ############################################################################################################################
    
    # Make a response object to set a custom cookie
    resp = make_response(send_file(f"{os.getcwd()}/export/data_templates/{file_prefix}-TEMPLATE.xlsx", as_attachment=True, download_name=f'{file_prefix}-TEMPLATE.xlsx'))

    # Set a cookie to let browser know that the file has been sent
    resp.set_cookie('template_file_sent', 'true', max_age=1)

    print("End Templater")

    return resp


