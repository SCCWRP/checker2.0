from flask import current_app
from inspect import currentframe
from .functions import checkData, get_badrows
from copy import deepcopy
import pandas as pd

def meta(all_dfs):
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should have tbl_testsite, bmpinfo, and monitoringstation
    testsite = all_dfs['tbl_testsite']
    bmp = all_dfs['tbl_bmpinfo']
    ms = all_dfs['tbl_monitoringstation']
    watershed = all_dfs['tbl_watershed']
    
    errs = []
    warnings = []

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    args = {
        "dataframe": pd.DataFrame({}),
        "tablename": 'tbl_test',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }


    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": get_badrows(df[df.temperature != 'asdf']),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]



    # (1) 
    # bmp name shouldnt have a comma in it
    args.update({
        "dataframe": bmp, 
        "tablename": 'tbl_bmpinfo',
        # Commented out code is what i think will work - Robert
        #"badrows": get_badrows(bmp[bmp.bmpname.apply(lambda x: ',' in x)]),
        "badrows": get_badrows(bmp[bmp['bmpname'].isin([x for x in bmp.bmpname.values if "," in x])]),
        "badcolumn": "bmpname",
        "error_type" : "Invalid BMP Name",
        "error_message" : "BMP Names may not contain commas."
    })
    errs = [*errs, checkData(**args)]


    # (2)
    # Upstream BMP names must be found within the list of BMP names that they are submitting
    bad_upstreambmp = list(
        set([x.strip() for item in bmp['upstreambmpnames'].dropna().values for x in item.split(",")])  
        -
        set(bmp['bmpname'].values)
    )
        
    df_badrows = bmp[bmp['upstreambmpnames'].isin(
        set([x for x in bmp['upstreambmpnames'].dropna().values for y in bad_upstreambmp if y in x]) 
    )]
    args.update({
        "dataframe": bmp, 
        "tablename": 'tbl_bmpinfo',
        "badrows": get_badrows(df_badrows),
        "badcolumn": "upstreambmpnames",
        "error_type" : "Invalid BMP Name",
        "error_message" : "There is a BMP name in your list of upstream BMP names, which did not appear in the BMPName column"
    })
    errs = [*errs, checkData(**args)]


    # (3)

    # sitecode_measurementtype = {
    #     tmpsitename: 
    #     set([
    #         x.strip() 
    #         for item in ms[ms.sitename ==tmpsitename].measurementtype.dropna().values  
    #         for x in item.split(",")
    #     ]) 
    #     for tmpsitename in ms.sitename.unique()    
    # }

    sitecode_measurementtype = {
        x :
        set(
            sum(
                [
                    # mtypes = measurementtypes
                    list(map(lambda mtypes: mtypes.strip(), mtypes.split(","))) 
                    
                    # fillna with empty string here since it's making them np.NaN's when we expect a string
                    for mtypes in y['measurementtype'].fillna('').values
                ],
                []
            )
        ) 
        for x,y in ms.groupby('sitename')
    }
    bad_sitename = [x for x in sitecode_measurementtype.keys() if "P" not in sitecode_measurementtype[x]]
    
    df_badrows = ms[ms['sitename'].isin(bad_sitename)]
    args.update({
        "dataframe": ms, 
        "tablename": 'tbl_monitoringstation',
        "badrows": get_badrows(df_badrows),
        "badcolumn": "measurementtype,sitename",
        "error_type" : "No rain gauge",
        "error_message" : "This SiteName does not have a rain gauge"
    })
    errs = [*errs, checkData(**args)]

    # (4)
    df_badrows = bmp[
        bmp.apply(
            lambda row:
            (row['sitename'], row['bmpname'])
            not in
            tuple(zip(ms.sitename, ms.bmpname))
            ,
            axis = 1
        )
    ]
    args.update({
        "dataframe": bmp,
        "tablename": "tbl_bmpinfo",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "sitename,bmpname",
        "error_type" : "No matching sitename,bmpname",
        "error_message" : (
            "This record did not have a matching record in the MonitoringStation tab. "
            "Records are matched on SiteName and BMPName"
        )
    })
    errs = [*errs, checkData(**args)]
    
    # (5) 
    df_badrows = ms[
        ms.apply(
            lambda row:
            (row['sitename'], row['bmpname'])
            not in
            tuple(zip(bmp.sitename, bmp.bmpname))
            ,
            axis = 1
        )
    ]
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "sitename,bmpname",
        "error_type" : "Record mismatch",
        "error_message" : (
            "This record did not have a matching record in the BMP Info tab. "
            "Records are matched on SiteName and BMPName"
        )
    })
    errs = [*errs, checkData(**args)]

    # (6)
    df_badrows = testsite[testsite['sitename'].isin(list(set(testsite['sitename']) - set(bmp['sitename'])) )]
    args.update({
        "dataframe": testsite,
        "tablename": "tbl_testsite",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "error_message" : "This test site name did not show up in the BMP Info tab."
    })
    errs = [*errs, checkData(**args)]

    # (7)
    df_badrows = bmp[bmp['sitename'].isin(list(set(bmp['sitename']) - set(testsite['sitename'])) )]
    args.update({
        "dataframe": bmp,
        "tablename": "tbl_bmpinfo",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "error_message" : "This test site name did not show up in the Test Site tab."
    })
    errs = [*errs, checkData(**args)]

    # (8)
    # We may need to only run this if it passes the logic check 
    # that each record in monitoringstation has a corresponding record in bmpinfo
    df_badrows = ms[
        ms.apply(
            lambda row: 
            all([
                row['stationtype'] == "Bypass",
                *[
                    pd.isnull(x) 
                    for x in
                    bmp[
                        (bmp.bmpname == row['bmpname']) 
                        & (bmp.sitename == row['sitename'])
                    ] \
                    .upstreambmpnames \
                    .replace('',pd.NA) \
                    .values
                ]
            ]) 
            ,
            axis=1
        )
    ]
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "stationtype",
        "error_type" : "Logic Error",
        "error_message" : (
            "This Monitoring Station is a bypass " 
            "but it is associated with a BMP which is in the middle of a treatment train sequence"
        )
    })
    errs = [*errs, checkData(**args)]
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "stationtype",
        "error_type" : "Logic Error",
        "error_message" : (
            "This Monitoring Station is a bypass " 
            "but it is associated with a BMP which is in the middle of a treatment train sequence"
        )
    })
    errs = [*errs, checkData(**args)]

    # (9) in watershed.py

    # (10)
    sitecode_measurementtype = {
        x : 
        set(
            sum([
                list(map(lambda x: x.strip(), x.split(","))) 
                for x in y['measurementtype'].fillna('').values
            ],[])
        ) 
        for x,y in ms.groupby('sitename')
    }
    bad_measurement_type = list(
        set(*sitecode_measurementtype.values()) 
        -
        set(pd.read_sql("SELECT measurementtype FROM lu_measurementtype",current_app.eng).measurementtype.values)
    )
        
    ms['badrows'] = ms.apply(
        lambda row: ",".join([
            x for x in bad_measurement_type if x in row['measurementtype']
        ]) 
        if len([x for x in bad_measurement_type if x in row['measurementtype']]) > 0  
        else pd.NA, 
        axis=1
    )
    df_badrows = ms[~ms['badrows'].isnull()]

    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "measurementtype",
        "error_type" : "Lookup Fail",
        "error_message" : "Entry not found in lookup list."
    })
    errs = [*errs, checkData(**args)]

    # (11)
    duplicate_stationname = {
        x :
        [
            x for x in y['stationname'].values 
            if y['stationname'].to_list().count(x) > 1
        ] 
        for x,y in ms.groupby('sitename')
    }

    df_badrows = ms[
        ms.apply(
            lambda row: 
            row['stationname'] in duplicate_stationname[row['sitename']]
            ,
            axis=1
        )
    ]
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "measurementtype",
        "error_type" : "Duplicate Submission Error",
        "error_message" : "Duplicates found for StationName. Monitoring StationNames must be unique for test site."
    })
    errs = [*errs, checkData(**args)]


    
    # I think this code commented out wont work because the second mask is of a greater length than the dataframe it is filtering - Robert
    # Let me know if that is not clear

    # df_badrows = watershed[
    #         ~watershed['insitusoil'].isnull()
    #     ][
    #         watershed['insitusoil'].isin(
    #             [x for x in watershed['insitusoil'] if x not in lu_insitusoil]
    #         )
    #     ]
    
    lu_insitusoil = pd.read_sql("SELECT * FROM lu_insitusoil", current_app.eng).insitusoil.to_list() 
    df_badrows = watershed[
            # I replace empty string with none since i dont want an empty string to be falsely interpreted as a non missing value
            watershed.insitusoil.replace('', None).apply(lambda x: (not pd.isnull(x)) and (x not in lu_insitusoil))
        ]

    args.update({
        "dataframe": watershed, 
        "tablename": "tbl_watershed",
        "badrows": get_badrows(df_badrows),
        "badcolumn": "insitusoil",
        "error_type" : "Lookup Fail",
        "error_message" : "Entry is not in lookup list (A, B, C, or D)"
    })
    errs = [*errs, checkData(**args)]


    errs = [e for e in errs if len(e) > 0]
    warnings = [w for w in warnings if len(w) > 0]
    
    return {'errors': errs, 'warnings': warnings}