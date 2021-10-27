# This file is for various utilities to preprocess data before core checks

from flask import current_app, g
import pandas as pd
import re

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
            lambda x: x.astype(str).str.strip()
        )
        all_dfs[f"{table_name}"] = table_df
    print("END Stripping whitespace function")
    print(all_dfs)
    return all_dfs

def fix_case(all_dfs: dict):
    print("BEGIN fix_case function")
    for table_name in all_dfs.keys():
        table_df = all_dfs[f'{table_name}'] 
    #Among all the varchar cols, only get the ones tied to the lookup list
        lookup_sql = f"""
            SELECT
                kcu.column_name, 
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
            AND tc.table_name='{table_name}'
            AND ccu.table_name LIKE 'lu_%%';
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
                if item.lower() in list(map(str.lower,foreignkeys_luvalues[x]))
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
                  if item.lower() == new_item.lower()
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



def clean_data(all_dfs):

    all_dfs = strip_whitespace(all_dfs)
    all_dfs = fix_case(all_dfs)
    all_dfs = clean_speciesnames(all_dfs)
    all_dfs = fill_speciesnames(all_dfs)
    # all_dfs = hardcoded_fixes(all_dfs) # That one is customized for BMP at this moment

    return all_dfs