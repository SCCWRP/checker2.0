# This file is for various utilities to preprocess data before core checks

from flask import current_app, g
import pandas as pd


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
        # Among all the varchar cols, only get the ones tied to the lookup list -- modified to only find lu_lists that are not of numeric types
        lookup_sql = f"""
            SELECT
                con.conname AS constraint_name,
                src_table.relname AS source_table,
                src_col.attname AS source_column,
                tgt_table.relname AS foreign_table,
                tgt_col.attname AS foreign_column
            FROM
                pg_constraint con
                JOIN pg_class src_table ON con.conrelid = src_table.oid
                JOIN pg_class tgt_table ON con.confrelid = tgt_table.oid
                JOIN unnest(con.conkey) WITH ORDINALITY AS src_col_nums(src_attnum, ord)
                    ON true
                JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_col_nums(tgt_attnum, ord)
                    ON src_col_nums.ord = tgt_col_nums.ord
                JOIN pg_attribute src_col ON src_col.attrelid = src_table.oid AND src_col.attnum = src_col_nums.src_attnum
                JOIN pg_attribute tgt_col ON tgt_col.attrelid = tgt_table.oid AND tgt_col.attnum = tgt_col_nums.tgt_attnum
            WHERE
                con.contype = 'f'
                AND src_table.relname = '{table_name}';
        """
        lu_info = pd.read_sql(lookup_sql, g.eng)
           
        # The keys of this dictionary are the column's names in the dataframe, values are their lookup values
        foreignkeys_luvalues = {
            x : y for x,y in zip(
                lu_info.source_column,
                [
                    pd.read_sql(f"SELECT {lu_col} FROM {lu_table}",  g.eng)[f'{lu_col}'].to_list() 
                    for lu_col,lu_table 
                    in zip (lu_info.foreign_column, lu_info.foreign_table) 
                ]
            ) 
        }
        # Get their actual values in the dataframe
        foreignkeys_rawvalues = {
            x : [
                item 
                for item in table_df[x] 
                if str(item).lower() in list(map(str.lower, foreignkeys_luvalues[x] )) # bug: 'lower' for 'str' objects doesn't apply to 'int' object
            ]  
            for x in lu_info.source_column
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
    
    return all_dfs


def clean_data(all_dfs):
    print("preprocessing")
    print("strip whitespace")
    all_dfs = strip_whitespace(all_dfs)
    print("done stripping whitespace")
    
    print("fix case")
    # fix for lookup list values too, match to the lookup list value if case insensitivity is the only issue
    all_dfs = fix_case(all_dfs)                
    print("Done fixing case")

    print("all_dfs['tbl_waterquality'].fractionname.unique()")
    print(all_dfs['tbl_waterquality'].fractionname.unique())
    # all_dfs = hardcoded_fixes(all_dfs)

    print('done')
    return all_dfs




