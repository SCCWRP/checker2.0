# Templater routine dynamically calls lookup lists from database to populate into the data templates.
# The updated templates will be outputted to the user with all tables and the lookup lists. 
#below is from microplastics
#from flask import send_from_directory, render_template, request, redirect, Response, jsonify, send_file, json, current_app
#below is from empa main.py
from pandas.io import excel
from flask import render_template, request, jsonify, current_app, Blueprint, session, g, send_file
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine, text, exc, Table, MetaData
import urllib, json
import pandas as pd
from pandas import DataFrame
import numpy as np
import re
import os
from os import environ
import xlsxwriter
import openpyxl
from openpyxl import load_workbook

# dynamic lookup lists to template
# skip the formatting

templater = Blueprint('templater', __name__)
@templater.route('/templater', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
# consider using the app.datasets dictionary to generalize the code better
def template():
    print("start templater")
    datatype = request.args.get("datatype")
    print(datatype)
    #<-- maybe define app.secret_key here 
    eng = g.eng
    meta = MetaData()
    sql = eng.execute("SELECT conrelid::regclass AS table_from, conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE contype IN ('f', 'p') AND connamespace = 'sde'::regnamespace AND conname LIKE 'tbl%%' ORDER BY conrelid::regclass::text, contype DESC")
    sql_df = DataFrame(sql.fetchall())
    sql_df.columns = sql.keys()
    df = sql_df
    del sql_df

    df = df[df['table_from'].str[:4].str.contains('tbl_')]
    grouped_df = df.groupby('table_from')
    
    for key, item in grouped_df:
        print(grouped_df.get_group(key), "\n\n")
    
    descr_grouped_df = grouped_df.describe()
    print(descr_grouped_df)
    # dtypes is a list of datatypes for empa (in order by SOP No.)
    dtype = [
        'logger',
        'discretewq',
        'nutrients_lab',
        'nutrients_field',
        'edna_field',
        'edna_lab',
        'sedimentgrainsize_field',
        'sedimentgrainsize_lab'
        'benthicinfauna_field',
        'benthicinfauna_lab',
        'macroalgae',
        'sav',
        'bruv_field',
        'bruv_lab',
        'fishseines',
        'crabtrap',
        'vegetation',
        'feldspar'
    ]
    # if/else statements to create list of tables contained within template for each datatype
    # SOP 1 WQ Logger
    if datatype == 'logger':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_wq_logger_metadata',
            'tbl_logger_ctd_data',
            'tbl_logger_mdot_data',
            'tbl_logger_troll_data',
            'tbl_logger_tidbit_data'
            ]
    # SOP 2 Discrete WQ
    elif datatype == 'discretewq':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_waterquality_metadata',
            'tbl_waterquality_data'
            ]
    # SOP 3 Nutrients
    elif datatype == 'nutrients_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_nutrients_metadata'
            ]
    elif datatype == 'nutrients_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_nutrients_labbatch_data',
            'tbl_nutrients_data'
            ]
    # SOP 4 eDNA
    elif datatype == 'edna_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_edna_metadata'
            ]
    elif datatype == 'edna_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_edna_water_labbatch_data',
            'tbl_edna_sed_labbatch_data',
            'tbl_edna_data'
            ]
    # SOP 5 Sediment Grain Size
    elif datatype == 'sedimentgrainsize_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sedgrainsize_metadata'
            ]
    elif datatype == 'sedimentgrainsize_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sedgrainsize_data',
            'tbl_sedgrainsize_labbatch_data'
            ]
    # SOP 6 Benthic Infauna
    elif datatype == 'benthicinfauna_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_benthicinfauna_metadata'
            ]
    elif datatype == 'benthicinfauna_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_benthicinfauna_labbatch',
            'tbl_benthicinfauna_abundance',
            'tbl_benthicinfauna_biomass'
            ]
    # SOP 7 Macroalgae
    elif datatype == 'macroalgae':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_macroalgae_sample_metadata',
            'tbl_algaecover_data',
            'tbl_floating_data'
            ]
    # SOP 7 SAV
    elif datatype == 'sav':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_sav_metadata',
            'tbl_savpercentcover_data'
            ]
    # SOP 8 BRUV
    elif datatype == 'bruv_field':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_bruv_metadata',
            ]
    elif datatype == 'bruv_lab':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_bruv_data'
            ]
    # SOP 9 Fish Seines
    elif datatype == 'fishseines':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_fish_sample_metadata',
            'tbl_fish_abundance_data',
            'tbl_fish_length_data'
            ]
    # SOP 10 Crab Trap
    elif datatype == 'crabtrap':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_crabtrap_metadata',
            'tbl_crabfishinvert_abundance',
            'tbl_crabbiomass_length'
            ]
    # SOP 11 Vegetation
    elif datatype == 'vegetation':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_vegetation_sample_metadata',
            'tbl_vegetattivecover_data',
            'tbl_epifauna_data'
            ]
    # SOP 13 Feldspar
    elif datatype == 'feldspar':
        tbls = [
            'tbl_protocol_metadata',
            'tbl_feldspar_metadata',
            'tbl_feldspar_data'
            ]
    else:
        tbls = []
    keys_from_db = []

    for key, item in grouped_df:
        if key in tbls:
            print(key)
            print(item['pg_get_constraintdef'])
            keys_from_db.append(item['pg_get_constraintdef'])
    
    list_of_keys = []
    list_of_lu_needed = []
    for fkey in keys_from_db:
        print("Printing fkey: ")
        print(fkey)

        for lu in fkey:
            print("Printing lu in fkey: ")
            print(lu)
            print("appending list_of_keys: ")
            list_of_keys.append(lu)
    print("LIST OF KEYS: ")
    print(list_of_keys)
    primarykeylist = list()
    tmplist = list()
    for line in list_of_keys:
        # GET PRIMARY KEY COLUMNS FROM DATABASE AND ADD TO PRIMARYKEYCOLUMN LIST
        if "PRIMARY" in line:
            print(line)
            tmpprimary = line.split()[2:]
            for primary in tmpprimary:
                primary = re.sub('\(|\)|\,|','',primary)
                primarykeylist.append(primary)
        for element in line.split():
            if element.startswith('lu_'):
                print(f"The following is a lookup list: {element}")
                # why is the following line run? -- check these lu's in microplastics -- if applicable to empa, adjust accordingly, else bye
                ## commenting out next 2 lines (for now)
                #if element.split('(')[0] in ['lu_station', 'lu_stations', 'lu_organismalgae']:
                #    continue
                list_of_lu_needed.append(element.split('(')[0])
            if element.startswith('('):
                foreignkey = element.strip('()')
                # GET FOREIGN KEY COLUMNS FROM THE DATABASE AND ADD TO tmplist
                tmplist.append(foreignkey)
    print("THIS IS FOREIGN KEY COLUMNS(TBLS) + PRIMARY KEY COLUMNS(LOOK_UP) LIST")
    print(tmplist)
    # Currently at this point, the tmplist has only foreign key columns for the tabs/tbls. This list will be updated with the primary key columns(look-up list) later.
    print("THIS IS PRIMARY KEY COLUMN(TBLS) LIST")
    print(primarykeylist)
    list_of_lu_needed = list(set(list_of_lu_needed))

    print(list_of_lu_needed)

    lu_list_df = []
    for i, lu_list in enumerate(list_of_lu_needed):
        print(i, lu_list)
        sql = eng.execute("SELECT * FROM " + lu_list)
        sql_df = DataFrame(sql.fetchall())
        sql_df.columns = sql.keys()
        lu_list_df.append(sql_df)
        del sql_df
    del i, lu_list

    print("export file")
    # these template files are gonna have to be renames -- for ease, keep consist w/ datatype variable
    # # change names in the uploaded files in /export folder
    print(f"datatype: {datatype}, {datatype.upper()}")
    #excel_file = f"/var/www/empa/checker/export/routine/{datatype.upper()}-TEMPLATE.xlsx"
    excel_file = f"{os.getcwd()}/export/routine/{datatype.upper()}-TEMPLATE.xlsx"
    print(f"excel file: {excel_file}")
    options = {}
    options['strings_to_formulas'] = False
    options['strings_to_url'] = False
    #excel_writer = pd.ExcelWriter(excel_file, engine='xlsxwriter', options=options)
    #new attempt code start
    print("Loading workbook...")
    #book = load_workbook(excel_file)

    #changing engine to openpyxl
    #excel_writer = pd.ExcelWriter(excel_file, engine='xlsxwriter', options=options)
    #moving workbook variable before excel_writer defined
    print("loading workbook...")
    workbook = openpyxl.load_workbook(f"{os.getcwd()}/export/routine/{datatype.upper()}-TEMPLATE-old.xlsx")
    '''
    try: 
        workbook = openpyxl.load_workbook(filename=excel_file)
    except: 
        workbook = openpyxl.Workbook()
        workbook.save(excel_file)
        workbook = openpyxl.load_workbook(filename=excel_file)
    '''
    print("loaded workbook")
    print("populating excel_writer")
    excel_writer = pd.ExcelWriter(excel_file, engine='openpyxl') #, options=options)
    print("populating excel_writer -- done")
    #file_name = excel_file
    #print(f"file name: {file_name}")
    #workbook = load_workbook(filename = file_name)
    #print("loaded workbook")
    #excel_writer.book = book
#    print("creating workbook")
    #workbook = excel_writer.book # commenting out because we want to append the file maybe question mark
    #print(" ===== created workbook =====")
    print("reading the template in")
    print(f"datatype: {datatype}")
    # does this need a conditional to display bad datatype --- might be a good idea
    if datatype in dtype:
        print("before xls")
        xls = pd.ExcelFile(f"{os.getcwd()}/export/routine/{datatype.upper()}-TEMPLATE-old.xlsx")
        
    # Initialize
    list_of_df = []
    original_df = []

    print("The following are the sheetnames of xls: ")
    print(xls.sheet_names)

    # Append keys
    print("Start appending keys...")
    for i, sheet in enumerate(xls.sheet_names):
        print(f"list_of_df: {list_of_df}")
        list_of_df.append(pd.read_excel(xls, sheet_name = sheet))
        print(f"sheetname: {sheet}")
        original_df.append(pd.read_excel(xls, sheet_name = sheet))
        # Lowercase
        list_of_df[i].columns = [str(x).lower() for x in list_of_df[i].columns]
    print("exit for loop -- append keys done")
    del i, sheet

    # Skipping formatting part of templater routine provided by microplastics/smc.
    # Export original_df to the excel file
    # just append the original file...
    '''
    for i in range(len(original_df)):
        original_df[i].to_excel(excel_writer, sheet_name = xls.sheet_names[i], startrow=1, index=False, header=False)
        workbook = excel_writer.book
        worksheet = excel_writer.sheets[xls.sheet_names[i]]
    '''
    print("using template-not old")
    if os.path.exists(excel_file):
        print("this exists why is it complaining...")
    else: 
        print("why doesnt it exist ... ")
    #openpyxl load_workbook issue: zipfile.BadZipFile: File is not a zip file

    #Change: import fcn alone and then call # did not work
    print(excel_file)
    workbook = openpyxl.Workbook()
    workbook = load_workbook("/var/www/checker/export/routine/LOGGER-TEMPLATE.xlsx")
    #excel_writer.book = openpyxl.load_workbook(filename=excel_file)
    print("this workedddd") # no it did not
    # Add lookup lists to the excel file.
    print("Start adding lookup lists to the excel file...")
    for i, lu_list in enumerate(list_of_lu_needed):
        print(f"i: {i}, lu_list: {lu_list}")
        sql_df = pd.read_sql(f"SELECT * FROM {lu_list};", eng)
        sql_df = sql_df[[x for x in sql_df.columns if x not in current_app.system_fields]]
        print(f'sql_df: {sql_df}')
        print(type(sql_df))
        #sql_df.to_excel(excel_writer, sheet_name = lu_list, startrow=1, index=False, header=False)
        #workbook = excel_writer.book
        print("adding to workbook -- lookup list above")
        #lu_list_name = f'{lu_list}'
        #print(type(lu_list_name))
        #workbook = excel_writer.sheets[lu_list_name]
        workbook.create_sheet(f'{lu_list}')
        sql_df.to_excel(excel_writer, sheet_name = lu_list, startrow=1, index=False, header=False)
        print(f"{lu_list} added to excel...")
        # change the following to one sheet with lu_list as columns within the sheet
        # # This may need further modifcation for lookup lists with multicolumn checks. 
        #worksheet = excel_writer.sheets[lu_list]
    print("saving excel writer")
    excel_writer.save()
    excel_writer.close()
    print(" ====== saved =====")
    # probably better to generalize with the below code, but for testing right now let's just call the exact file path and modify
    #return send_file(f'{os.getcwd()}/export/{datatype.upper()}-TEMPLATE.xlsx', as_attachment=True, attachment_filename=f'{datatype.upper()}-TEMPLATE.xlsx')
    return send_file(f'{os.getcwd()}/export/routine/{datatype.upper()}-TEMPLATE.xlsx', as_attachment=True, attachment_filename=f'{datatype.upper()}-TEMPLATE.xlsx')




