# This file is for various utilities to preprocess data before core checks

from flask import current_app, g
import pandas as pd
import re
import time
import numpy as np

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

#revise fill_empty_cells with 
## qry = select * from information_schema.columns WHERE table_name = {table_name} 
# for table_name in all_dfs.keys():
# SELECT 
#      column_name as col_name, 
#       udt_name as dt
# FROM information_schema.columns
#       WHERE table_name='{table_name}'
# table_sql = <qry>
# table_info = pd.read_sql(table_sql, g.eng)
# make sure no system_fields -- NOT IN app.system fields (see above)
# Datatypes are retrieved from information schema to populate empty cells with -88 or 'Not recorded' for the fill_empty_cells function.
'''
def fill_empty_cells(all_dfs):
    for table_name in all_dfs.keys():
        table_df = all_dfs[f'{table_name}']
        for col in table_df.columns:
            print("col: ", col)
            dt = table_df[col].dtype
            #if dt == object: #fillna method seems to not be doing what is should :/
                #table_df[col].fillna("", inplace = True)
                #table_df[col] = table_df[col].fillna('')
                #table_df[col].replace(np.nan, '', inplace = True)
                #table_df[col].replace('NA', '', inplace = True)
                #table_df[col] = table_df[col].replace(np.nan, '', regex = True)
                #table_df[col] = table_df[col].replace(np.NaN, '', regex = True)
                #print("table_df[col]")
                #print(table_df[col])
                #time.sleep(3)
            #print(table_df[col].isna())
            #time.sleep(3)
            print("dt: ", dt)
            # if dt in ['int2','int4','numeric','timestamp']:
            if dt == np.float64 or dt == np.int64: #numeric data type fills correctly!
                table_df[col].fillna(-88, inplace = True)
            else: # meaning dt in ['varchar']
                table_df[col].fillna("Not recorded", inplace = True)

            #print("table_df subset null")
            #print(table_df[table_df[col].isnull()]) #all of these dfs returned empty >:(
            #print(table_df[table_df[col].isna()])
            #time.sleep(3)
        all_dfs[f'{table_name}'] = table_df
    return all_dfs
'''
#revised fill_empty_cells - zaib
def fill_empty_cells(all_dfs):
    for table_name in all_dfs.keys():
        table_df = all_dfs[f'{table_name}']
        table_sql = f"""
            SELECT 
                column_name as col_names,
                udt_name as udt
            FROM 
                information_schema.columns
            WHERE
                table_name='{table_name}'
            AND column_name NOT IN ('{"','".join(current_app.system_fields)}');
            """
        table_info = pd.read_sql(table_sql, g.eng)
        for col in table_df.columns:
            print("col: ", col)
            #dt = table_df[col].dtype
            #if dt == object: #fillna method seems to not be doing what is should :/
                #table_df[col].fillna("", inplace = True)
                #table_df[col] = table_df[col].fillna('')
                #table_df[col].replace(np.nan, '', inplace = True)
                #table_df[col].replace('NA', '', inplace = True)
                #table_df[col] = table_df[col].replace(np.nan, '', regex = True)
                #table_df[col] = table_df[col].replace(np.NaN, '', regex = True)
                #print("table_df[col]")
                #print(table_df[col])
                #time.sleep(3)
            #print(table_df[col].isna())
            #time.sleep(3)
            dt = table_info.loc[table_info['col_names']== col, 'udt'].iloc[0]
            print("dt: ", dt)
            if dt in ['int2','int4','numeric','timestamp']:
            #if dt == np.float64 or dt == np.int64: #numeric data type fills correctly!
                table_df[col].fillna(-88, inplace = True)
            else: # meaning dt in ['varchar']
                table_df[col].fillna("Not recorded", inplace = True)

            #print("table_df subset null")
            #print(table_df[table_df[col].isnull()]) #all of these dfs returned empty >:(
            #print(table_df[table_df[col].isna()])
            #time.sleep(3)
        all_dfs[f'{table_name}'] = table_df
    return all_dfs

# because every project will have those non-generalizable, one off, "have to hard code" kind of fixes
# and this project is no exception
def hardcoded_fixes(all_dfs):
    if 'tbl_ceden_waterquality' in all_dfs.keys():

        # hard coded fix for analytename column for water quality
        all_dfs['tbl_ceden_waterquality']['analytename'] = all_dfs['tbl_ceden_waterquality'] \
            .analytename \
            .apply(
                lambda x:
                'Nitrogen, Total Kjeldahl'
                if 'kjeldahl' in str(x).lower()

                else 'Ammonia as N'
                if 'ammonia' in str(x).lower()

                else 'Total Organic Carbon'
                if 'organic carbon' in str(x).lower()

                else 'Hardness as CaCO3'
                if ('hardness' in str(x).lower()) and ('carbonate' in str(x).lower())

                else 'Nitrate as N'
                if 'nitrogen, nitrate (no3) as n' in str(x).lower()

                else 'SpecificConductivity'
                if 'specific conductance' in str(x).lower()

                else 'Nitrogen, Total'
                if str(x).lower() == 'nitrogen'

                else 'OrthoPhosphate as P'
                if 'orthophosphate' in str(x).lower()
                
                else 'Nitrate + Nitrite as N'
                if (('nitrate' in str(x).lower()) and ('nitrite' in str(x).lower()))

                else x
            )

    return all_dfs

'''
def clean_speciesnames(all_dfs):
    
    all_dfs['tbl_fish_length_data']['scientificname'] = all_dfs['tbl_fish_length_data']['scientificname'] \
        .apply(lambda x: re.sub("[\(|\)|\?|,]","", str(x)) )

    all_dfs['tbl_fish_length_data']['commonname'] = all_dfs['tbl_fish_length_data']['commonname'] \
        .apply(lambda x: re.sub("[\(|\)|\?|,]","", str(x)) )
    
    all_dfs['tbl_fish_abundance_data']['scientificname'] = all_dfs['tbl_fish_abundance_data']['scientificname'] \
        .apply(lambda x: re.sub("[\(|\)|\?|,]","", str(x)) )

    all_dfs['tbl_fish_abundance_data']['commonname'] = all_dfs['tbl_fish_abundance_data']['commonname'] \
        .apply(lambda x: re.sub("[\(|\)|\?|,]","", str(x)) )

    return all_dfs
    
def fill_speciesnames(all_dfs):
    lu_fishspecies = pd.read_sql('SELECT scientificname, commonname FROM lu_fishspecies', g.eng)
    names = {
        c: s  for s, c in list(zip(lu_fishspecies.scientificname, lu_fishspecies.commonname))
    }

    # I dont really know how to explain the code with comments to be honest but hopefully it makes sense
    all_dfs['tbl_fish_abundance_data']['scientificname'] = all_dfs['tbl_fish_abundance_data'] \
        .apply(
            lambda x:
            names[x['scientificname']] if (pd.isnull(x['commonname']) or x['commonname'] == '') else x['commonname']
            ,
            axis = 1
        )
    
    # here we need to get the key of the dictionary based on the value
    all_dfs['tbl_fish_abundance_data']['commonname'] = all_dfs['tbl_fish_abundance_data'] \
        .apply(
            lambda x:
            list(names.keys())[list(names.values()).index(x['scientificname'])] if (pd.isnull(x['scientificname']) or x['scientificname'] == '') else x['scientificname']
            ,
            axis = 1
        )

    return all_dfs
'''


def clean_data(all_dfs):
    print("Before strip whitespace and any preprocessing")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    #rint('\n')
    all_dfs = strip_whitespace(all_dfs)
    print("After strip whitespace")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    #print('\n')
    
    #disabled to test checks -- jk enabled to test submit data
    
    print("Before fix case")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    print('\n')
    all_dfs = fix_case(all_dfs)                # fix for lookup list values too, match to the lookup list value if case insensitivity is the only issue
    print("After fix case")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    print('\n')

    print("before filling empty values")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    print('\n')
    all_dfs = fill_empty_cells(all_dfs)
    print("after filling empty values")
    #print(all_dfs['tbl_fish_sample_metadata'][['siteid','estuaryname']])
    print('\n')
    
    #all_dfs = clean_speciesnames(all_dfs)
    #all_dfs = fill_speciesnames(all_dfs)
    #all_dfs = hardcoded_fixes(all_dfs) # That one is customized for BMP at this moment -- change to empa specific fixes

    return all_dfs