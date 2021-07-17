import json, os
from pandas import isnull, DataFrame

def checkData(dataframe, tablename, badrows, badcolumn, error_type, is_core_error = False, error_message = "Error", errors_list = [], q = None):
    
    # See comments on the get_badrows function
    # doesnt have to be used but it makes it more convenient to plug in a check
    # that function can be used to get the badrows argument that would be used in this function
    
    if len(badrows) > 0:
        if q is not None:
            # This is the case where we run with multiprocessing
            # q would be a mutliprocessing.Queue() 
            q.put({
                "table": tablename,
                "rows":badrows,
                "columns":badcolumn,
                "error_type":error_type,
                "is_core_error" : is_core_error,
                "error_message":error_message
            })

        return {
            "table": tablename,
            "rows":badrows,
            "columns":badcolumn,
            "error_type":error_type,
            "is_core_error" : is_core_error,
            "error_message":error_message
        }
    return {}
        

def get_badrows(df_badrows, errmsg):
    """
    df_badrows is a dataframe filtered down to the rows which DO NOT meet the criteria of the check. 
    errmsg is self explanatory
    """

    assert isinstance(df_badrows, DataFrame), "in function get_badrows, df_badrows argument is not a pandas DataFrame"
    assert isinstance(errmsg, str), f"in function get_badrows, errmsg argument ({errmsg}) is not a string"


    if df_badrows.empty:
        return []

    return [
        {
            # row number is the row number in the excel file
            'row_number': int(rownum),
            'value': val if not isnull(val) else '',
            'message': msg
        } 
        for rownum, val, msg in
        df_badrows \
        .apply(
            lambda row:
            (
                row.name,

                # We wont be including the specific cell value in the error message for custom checks, 
                # it would be too complicated to implement in the cookie cutter type fashion that we are looking for, 
                # since the cookie cutter model that we have with the other checker proved effective for faster onboarding of new people to writing their own checks. 
                # Plus in my opinion, the inclusion of the specific value is really mostly helpful for the lookup list error. 
                # The only reason why the dictionary still includes this item is for the sake of consistency - 
                # (all the other "badrows" dictionaries are formatted in this way, since there are a few error types in core checks where the specific cell value was included.) 
                # This is ok since Core checks is 99.9% not going to change or have any additional features added, 
                # thus we dont need to make it super convenient for others to add checks

                # Note that for this "get_badrows" function, it works essentially the same way as the previous checker, 
                # where the user basically provides a line of code to subset the dataframe, along with an accompanying error message
                None, 
                errmsg
            ),
            axis = 1
        ) \
        .values
    ]


