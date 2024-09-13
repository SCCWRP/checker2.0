from flask import current_app, g
from inspect import currentframe
from .functions import checkData
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
    catchment = all_dfs['tbl_catchment']
    
    
    # add tmp_row column - a copy of the index which ensures accuracy of assigning errors to rows
    testsite['tmp_row'] = testsite.index
    bmp['tmp_row'] = bmp.index
    ms['tmp_row'] = ms.index
    catchment['tmp_row'] = catchment.index
    
    
    
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
    #   "badrows": df[df.tmperature != 'asdf']),
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
        "badrows": bmp[bmp.bmpname.apply(lambda x: ',' in str(x))].tmp_row.tolist(),
        "badcolumn": "bmpname",
        "error_type" : "Invalid BMP Name",
        "error_message" : "BMP Names may not contain commas."
    })
    errs = [*errs, checkData(**args)]


    # (2)
    # Upstream BMP names must be found within the list of BMP names that they are submitting
    bad_upstreambmp = (
        set([x.strip() for item in bmp['upstreambmpnames'].dropna().values for x in item.split(",")])  
        -
        set(bmp['bmpname'].values)
    )
    
    badrows = bmp[bmp['upstreambmpnames'].isin(
        set([x for x in bmp['upstreambmpnames'].dropna().values for y in bad_upstreambmp if y in x]) 
    )].tmp_row.tolist()

    args.update({
        "dataframe": bmp, 
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "upstreambmpnames",
        "error_type" : "Invalid BMP Name",
        "error_message" : "There is a BMP name in your list of upstream BMP names, which did not appear in the BMPName column"
    })
    errs = [*errs, checkData(**args)]


    # (3) Each site needs a rain gauge
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
    
    badrows = ms[ms['sitename'].isin(bad_sitename)].tmp_row.tolist()
    args.update({
        "dataframe": ms, 
        "tablename": 'tbl_monitoringstation',
        "badrows": badrows,
        "badcolumn": "measurementtype,sitename",
        "error_type" : "No rain gauge",
        "error_message" : "This SiteName does not have a rain gauge"
    })
    errs = [*errs, checkData(**args)]

    # (4) Each record in the BMP tab needs a corresponding record in the Monitoring Station tab 
    # (This method is outdated and needs to be updated)
    badrows = bmp[
        bmp.apply(
            lambda row:
            (row['sitename'], row['bmpname'])
            not in
            tuple(zip(ms.sitename, ms.bmpname))
            ,
            axis = 1
        )
    ].tmp_row.tolist()
    args.update({
        "dataframe": bmp,
        "tablename": "tbl_bmpinfo",
        "badrows": badrows,
        "badcolumn": "sitename,bmpname",
        "error_type" : "No matching sitename,bmpname",
        "error_message" : (
            "This record did not have a matching record in the MonitoringStation tab. "
            "Records are matched on SiteName and BMPName"
        )
    })
    errs = [*errs, checkData(**args)]
    
    # (5) Each record in monitoringstation tab needs a corresponding record in the BMPInfo tab
    badrows = ms[
        ms.apply(
            lambda row:
            (row['sitename'], row['bmpname'])
            not in
            tuple(zip(bmp.sitename, bmp.bmpname))
            ,
            axis = 1
        )
    ].tmp_row.tolist()
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": badrows,
        "badcolumn": "sitename,bmpname",
        "error_type" : "Record mismatch",
        "error_message" : (
            "This record did not have a matching record in the BMP Info tab. "
            "Records are matched on SiteName and BMPName"
        )
    })
    errs = [*errs, checkData(**args)]

    # (6)
    badrows = testsite[testsite['sitename'].isin(list(set(testsite['sitename']) - set(bmp['sitename'])) )].tmp_row.tolist()
    args.update({
        "dataframe": testsite,
        "tablename": "tbl_testsite",
        "badrows": badrows,
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "error_message" : "This test site name did not show up in the BMP Info tab."
    })
    errs = [*errs, checkData(**args)]

    # (7)
    badrows = bmp[bmp['sitename'].isin(list(set(bmp['sitename']) - set(testsite['sitename'])) )].tmp_row.tolist()
    args.update({
        "dataframe": bmp,
        "tablename": "tbl_bmpinfo",
        "badrows": badrows,
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "error_message" : "This test site name did not show up in the Test Site tab."
    })
    errs = [*errs, checkData(**args)]

    # (8)
    # We may need to only run this if it passes the logic check 
    # that each record in monitoringstation has a corresponding record in bmpinfo
    badrows = ms[
        ms.apply(
            lambda row: 
            all([
                row['stationtype'] == "Bypass",
                *[
                    not pd.isnull(x) 
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
    ].tmp_row.tolist()
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": badrows,
        "badcolumn": "stationtype",
        "error_type" : "Logic Error",
        "error_message" : (
            "This Monitoring Station is a bypass, " 
            "but it seems like it is associated with a BMP which has upstream treatment"
        )
    })
    errs = [*errs, checkData(**args)]

    # (9) in catchment.py
    # badrows = catchment[~catchment.insitusoil.isin(pd.read_sql("SELECT insitusoil FROM lu_insitusoil",g.eng).insitusoil.values)]
    # args.update({
    #     "dataframe": ms,
    #     "tablename": "tbl_monitoringstation",
    #     "badrows": badrows,
    #     "badcolumn": "stationtype",
    #     "error_type" : "Logic Error",
    #     "error_message" : (
    #      
    #     )
    # })
    # errs = [*errs, checkData(**args)]


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
    bad_measurement_type = (
        set([val for s in sitecode_measurementtype.values() for val in s]) 
        -
        set(pd.read_sql("SELECT measurementtype FROM lu_measurementtype",g.eng).measurementtype.values)
    )
    
    ms['badrows'] = ms.apply(
        lambda row: ",".join([
            x for x in bad_measurement_type if x in row['measurementtype']
        ]) 
        if len([x for x in bad_measurement_type if x in row['measurementtype']]) > 0  
        else pd.NA, 
        axis=1
    )
    badrows = ms[~ms['badrows'].isnull()].tmp_row.tolist()

    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": badrows,
        "badcolumn": "measurementtype",
        "error_type" : "Lookup Fail",
        "error_message" : "Entry not found in lookup list."
    })
    errs = [*errs, checkData(**args)]



    # (11) A sitename may not have two of the same monitoring station name
    # Check removed - I do not think it is actually something that we will check for

    

    # NOTE THIS CHECK MAY ONLY APPLY IF insitusoilmeasuretype is NRCS

    # lu_insitusoil = pd.read_sql("SELECT * FROM lu_insitusoil", g.eng).insitusoil.to_list() 
    # badrows = catchment[
    #         # I replace empty string with none since i dont want an empty string to be falsely interpreted as a non missing value
    #         catchment.insitusoil.replace('', None).apply(lambda x: (not pd.isnull(x)) and (x not in lu_insitusoil))
    #     ] \
    #     .tmp_row.tolist()

    # args.update({
    #     "dataframe": catchment, 
    #     "tablename": "tbl_catchment",
    #     "badrows": badrows,
    #     "badcolumn": "insitusoil",
    #     "error_type" : "Lookup Fail",
    #     "error_message" : "Entry is not in lookup list (A, B, C, or D)"
    # })
    # errs = [*errs, checkData(**args)]


    # A check that needs to be written is that the BMP Purpose can have multiple values, but the values must come from lu_bmppurpose
    # A multi value lookup check

    errs = [e for e in errs if len(e) > 0]
    warnings = [w for w in warnings if len(w) > 0]
    print("Done with custom checks")
    return {'errors': errs, 'warnings': warnings}