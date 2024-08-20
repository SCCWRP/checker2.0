import pandas as pd
from .functions import checkData
from flask import current_app

# q is a multiprocessing.Queue()
# pass it in in the case that this is done with multiprocessing
# multitask function passes in a multiprocessing Queue() as the last argument for each function that gets passed into it
# therefore to pass a function to the multitask function we would need to allow for that queue to be passed into it
def checkLookUpLists(dataframe, tablename, eng, *args, output = None, **kwargs):
    print("BEGIN checkLookupLists")
    #assert dtype in tbl_tablenames.keys(), "Invalid Datatype in checkLookUpCodes function call"
    
    lu_list_script_root = current_app.script_root

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
        AND tc.table_name='{tablename}'
        AND ccu.table_name LIKE 'lu_%%';
    """

    # fkeys = foreign keys
    fkeys = pd.read_sql(lookup_sql, eng)
    # dont check lookup lists for columns that are not in unified table
    fkeys = fkeys[fkeys.column_name.isin(dataframe.columns)]
    # print("fkeys")
    # print(fkeys)
    # print("lookup_sql")
    # print(lookup_sql)


    out = [
        checkData(
            dataframe = dataframe, 
            tablename = tablename,
            badrows = dataframe[dataframe[col] == val].index.tolist(),
            badcolumn = col,
            error_type = 'Lookup List Fail',
            is_core_error = True,
            error_message = (
                f'This value you entered ({val}) did not match the lookup list '
                '<a '
                f'href="/{lu_list_script_root}/scraper?action=help&layer={fkeys[fkeys.column_name == col].foreign_table_name.values[0]}" '
                'target="_blank">'
                f'{fkeys[fkeys.column_name == col].foreign_table_name.values[0]}'
                '</a>'
            )
        )
            
        for col in dataframe.columns if col in fkeys.column_name.unique()
        for val in 
        dataframe[
            ~dataframe[col].isin(
                pd.read_sql(
                    (
                        f"SELECT {fkeys[fkeys.column_name == col].foreign_column_name.values[0]} "
                        f"FROM {fkeys[fkeys.column_name == col].foreign_table_name.values[0]};"
                    ),
                    eng
                )[fkeys[fkeys.column_name == col].foreign_column_name.values[0]] \
                .values
            )
        ][col] \
        .unique()
            
    ]

    if output:
        output.put(out)

    print("END checkLookupLists")
    return out