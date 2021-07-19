from .functions import checkData, get_badrows

def datalogger(all_dfs):
    errs = []
    warnings = []
    for tbl, df in all_dfs.items():
        if tbl == 'tbl_data_logger_raw':
            errs.extend(
                [
                    checkData(
                        dataframe = df,
                        tablename = tbl,
                        badrows = get_badrows(df[df.temperature != 'asdf'], "The value here is not asdf"),
                        badcolumn = 'temperature',
                        error_type = "Not asdf",
                        is_core_error = False,
                        error_message = "this is not asdf"
                        
                    )
                ]
            )
            warnings.extend(
                [
                    checkData(
                        dataframe = df,
                        tablename = tbl,
                        badrows = get_badrows(df[df.intensity != 5], "The value here is not 5"),
                        badcolumn = 'intensity',
                        error_type = "not 5",
                        is_core_error = False,
                        error_message = "this is not 5"
                        
                    )
                ]
            )
        if tbl == 'tbl_data_logger_metadata':
            warnings.extend(
                [
                    checkData(
                        dataframe = df,
                        tablename = tbl,
                        badrows = get_badrows(df[df.site_type != 5], "The value here is not 5"),
                        badcolumn = 'site_type',
                        error_type = "not 5",
                        is_core_error = False,
                        error_message = "this is not 5"
                        
                    )
                ]
            )
    
    return {'errors': errs, 'warnings': warnings}