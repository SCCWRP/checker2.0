# This file is for various utilities to preprocess data before core checks

from flask import current_app, g
import pandas as pd
import re
import time
import numpy as np

# its getting late and its Friday and i need to leave soon,
# I put this in a table called "lu_teststation"
# later i need to adjust the checker code to use that table rather 
test_station_renaming_key = {
    'B23-12000' : 'B23-TEST1',
    'B23-12321' : 'B23-TEST2',
    'B23-12177' : 'B23-TEST3',
    'B23-12044' : 'B23-TEST4',
    'B23-12217' : 'B23-TEST5',
    'B23-12194' : 'B23-TEST6',
    'B23-12196' : 'B23-TEST7',
    'B23-12063' : 'B23-TEST8',
    'B23-12013' : 'B23-TEST9',
    'B23-12064' : 'B23-TEST10',
    'B23-12113' : 'B23-TEST11'
}

def strip_whitespace(all_dfs: dict):
    print("BEGIN Stripping whitespace function")
    for table_name in all_dfs.keys():
        #First get all the foreign keys columns
        meta = pd.read_sql(
            f"""
            SELECT 
                table_name, 
                column_name, 
                is_nullable, 
                data_type,
                udt_name, 
                character_maximum_length, 
                numeric_precision, 
                numeric_scale 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = '{table_name}'
                AND column_name NOT LIKE 'login_%%'
                AND column_name NOT IN ('{"','".join(current_app.system_fields)}');
            """, 
             g.eng
        )
        
        meta[meta['udt_name'] == 'varchar']
        table_df = all_dfs[f'{table_name}'] 
        # Get all varchar cols from table in all_dfs
        all_varchar_cols = meta[meta['udt_name'] == 'varchar'].column_name.values
        
        # Strip whitespace left side and right side
        table_df[all_varchar_cols] = table_df[all_varchar_cols].apply(
            lambda col: col.apply(lambda x: str(x).strip() if not pd.isnull(x) else x)
        )
        all_dfs[f"{table_name}"] = table_df
    print("END Stripping whitespace function")
    return all_dfs

def fix_case(all_dfs: dict):
    print("BEGIN fix_case function")
    for table_name in all_dfs.keys():
        table_df = all_dfs[f'{table_name}'] 
    #Among all the varchar cols, only get the ones tied to the lookup list -- modified to only find lu_lists that are not of numeric types
        lookup_sql = f"""
            SELECT
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                isc.data_type AS column_data_type 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
                JOIN information_schema.columns as isc
                ON isc.column_name = ccu.column_name
                AND isc.table_name = ccu.table_name
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name='{table_name}'
            AND ccu.table_name LIKE 'lu_%%'
            AND isc.data_type NOT IN ('integer', 'smallint', 'numeric');
        """
        lu_info = pd.read_sql(lookup_sql, g.eng)
           
        # The keys of this dictionary are the column's names in the dataframe, values are their lookup values
        foreignkeys_luvalues = {
            x : y for x,y in zip(
                lu_info.column_name,
                [
                    pd.read_sql(f"SELECT {lu_col} FROM {lu_table}",  g.eng)[f'{lu_col}'].to_list() 
                    for lu_col,lu_table 
                    in zip (lu_info.foreign_column_name, lu_info.foreign_table_name) 
                ]
            ) 
        }
        # Get their actual values in the dataframe
        foreignkeys_rawvalues = {
            x : [
                item 
                for item in table_df[x] 
                if str(item).lower() in list(map(str.lower,foreignkeys_luvalues[x])) # bug: 'lower' for 'str' objects doesn't apply to 'int' object
            ]  
            for x in lu_info.column_name
        }
        
        # Remove the empty lists in the dictionary's values. 
        # Empty lists indicate there are values that are not in the lu list for a certain column
        # This will be taken care of by core check.
        foreignkeys_rawvalues = {x:y for x,y in foreignkeys_rawvalues.items() if len(y)> 0}
        
        # Now we need to make a dictionary to fix the case, something like {col: {wrongvalue:correctvalue} }
        # this is good indentation, looks good and easy to read - Robert
        fix_case  = {
            col : {
                  item : new_item 
                  for item in foreignkeys_rawvalues[col] 
                  for new_item in foreignkeys_luvalues[col] 
                  if str(item).lower() == new_item.lower()
            } 
            for col in foreignkeys_rawvalues.keys()
        }
        table_df = table_df.replace(fix_case)                
        all_dfs[f'{table_name}'] = table_df
    print("END fix_case function")
    return all_dfs



# because every project will have those non-generalizable, one off, "have to hard code" kind of fixes
# and this project is no exception
def hardcoded_fixes(all_dfs):
    if 'tbl_chemresults' in all_dfs.keys():
        all_dfs['tbl_chemresults']['units'] = all_dfs['tbl_chemresults'] \
            .apply(
                lambda row: str(row.units).replace('ug/kg ww','ng/g ww').replace('ug/kg dw','ng/g dw') if not ('Reference' in str(row.sampletype)) else row.units, 
                axis = 1
            )
    print('hardcorde fixes - done')
    return all_dfs


def clean_data(all_dfs):
    print("preprocessing")
    print("strip whitespace")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    #rint('\n')
    all_dfs = strip_whitespace(all_dfs)
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    #print('\n')
    
    #disabled to test checks -- jk enabled to test submit data
    
    # print("fix case")
    # fix for lookup list values too, match to the lookup list value if case insensitivity is the only issue
    # all_dfs = fix_case(all_dfs)                
    

    # all_dfs = hardcoded_fixes(all_dfs)

    print('done')
    return all_dfs


def rename_test_stations(all_dfs, login_email):
    print("renaming test stations")
    
    # Make the test_station_renaming_key (https://chat.openai.com/share/3f615ac3-2dd3-4dba-9456-3ff8f5530628)
    df = pd.read_sql("SELECT stationid, test_stationid FROM lu_teststation", g.eng)
    test_station_renaming_key = pd.Series(df.test_stationid.values,index=df.stationid).to_dict()

    if login_email == current_app.config.get('TESTING_EMAIL_ADDRESS'):
        for dfname, df in all_dfs.items():
            all_dfs[dfname] = df.replace(test_station_renaming_key)

    
    print('done')

    return all_dfs


def check_test_stations(all_dfs, login_email):
    errs = []
    if login_email != current_app.config.get('TESTING_EMAIL_ADDRESS'):
        return errs

    # Make the test_station_renaming_key (https://chat.openai.com/share/3f615ac3-2dd3-4dba-9456-3ff8f5530628)
    df = pd.read_sql("SELECT stationid, test_stationid FROM lu_teststation", g.eng)
    test_station_renaming_key = pd.Series(df.test_stationid.values,index=df.stationid).to_dict()


    for dfname, df in all_dfs.items():
        if 'stationid' in df.columns:
            badrows = df[ ~df.stationid.isin( [*list(test_station_renaming_key.keys()), *list(test_station_renaming_key.values())] ) ].index.tolist()

            if len(badrows) > 0:
                # same structure as output of checkData
                # Also i just found out, i dont have to use the script_root when i dont put a slash in from of "scraper"
                #   it automatically puts it after the script root part and handles the relative url the way we want
                errs.append({
                    "table": dfname,
                    "rows": badrows,
                    "columns": 'stationid',
                    "error_type": 'Value Error',
                    "is_core_error" : True,
                    "error_message": f"This is a test submission, but you are not using one of the designated <a href=scraper?action=help&layer=lu_teststation target=_blank>test stations</a> as a stationid. Your values may come from either the stationid or the test_stationid column."
                })
    return errs



