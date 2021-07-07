from .functions import checkData, get_badrows

def func1(all_dfs):
    errs = []
    warnings = []
    for tbl, df in all_dfs.items():
        errs.extend(
            [
                checkData(
                    dataframe = df,
                    tablename = tbl,
                    badrows = get_badrows(df, df.first != 'asdf', "The value here is not asdf"),
                    badcolumn = 'first',
                    error_type = "Not asdf",
                    is_core_error = False,
                    error_message = "this is not asdf"
                    
                )
            ]
        )
    
    return {'errors': errs, 'warnings': warnings}