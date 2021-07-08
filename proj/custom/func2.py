from .functions import checkData, get_badrows

def func2(all_dfs):
    errs = []
    warnings = []
    for tbl, df in all_dfs.items():
        errs.extend(
            [
                checkData(
                    dataframe = df,
                    tablename = tbl,
                    badrows = get_badrows(
                        df[df['first'] != "BSAHH"], 
                        "The value here is not BSAHH"
                    ),
                    badcolumn = 'first',
                    error_type = "Not BSAHH",
                    is_core_error = False,
                    error_message = "this is not BSAHH"
                )
            ]
        )
    
    return {'errors': errs, 'warnings': warnings}