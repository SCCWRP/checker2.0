import re
from .functions import checkData, get_primary_key
from pandas import isnull

# All the functions for the Core Checks should have the dataframe and the datatype as the two main arguments
# This is to allow the multiprocessing to work, so it can pass in the same args to all the functions
# Of course it would be possible to do it otherwise, but the mutlitask function we wrote in utils assumes 
# the case that all of the functions have the same arguments
def checkDuplicatesInSession(dataframe, tablename, eng, *args, output = None, **kwargs):
    """
    check for duplicates in session only
    """
    print("BEGIN function - checkDuplicatesInSession")
    
    pkey = get_primary_key(tablename, eng)
    print(pkey)

    if len(pkey) == 0:
        print("No Primary Key")
        return

    ret = []
    if any(dataframe.duplicated(pkey)):

        badrows = [
            {
                'row_number': int(rownum),
                'value': val if not isnull(val) else '',
                'message': msg
            } 
            for rownum, val, msg in
            dataframe[dataframe.duplicated(pkey, keep = False)] \
            .apply(
                lambda row:
                (
                    row.name + 1,
                    None, 
                    "This is a duplicated row"
                ),
                axis = 1
            ) \
            .values
        ]

        print("badrows")
        print(badrows)

        ret = [
            checkData(
                dataframe = dataframe,
                tablename = tablename,
                badrows = badrows,
                badcolumn = ','.join(pkey),
                error_type = "Duplicated Rows",
                is_core_error = True,
                error_message = "You have duplicated rows{}".format( 
                    f" based on the primary key fields {', '.join(pkey)}"
                )
            )
        ]

        if output:
            output.put(ret)

        
    print("END function - checkDuplicatesInSession")
    return ret



# TODO Check Duplicates in Production