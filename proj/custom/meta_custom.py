from flask import current_app, g
from inspect import currentframe
from .functions import checkData, mismatch
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

    eng = g.readonly_eng



    print("# ----------------------                         Logic Checks                                    ----------------------- #")
    ##############################################################################################################################
    # ----------------------                         Logic Checks                                    --------------------------- #
    ##############################################################################################################################



    # CHECK - Each record in Testsite needs a corresponding record in BMPinfo, matched on sitename
    print("#  CHECK - Each record in Testsite needs a corresponding record in BMPinfo, matched on sitename")
    badrows = mismatch(testsite, bmp, mergecols=['sitename'])
    args.update({
        "dataframe": testsite,
        "tablename": "tbl_testsite",
        "badrows": badrows,
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "is_core_error": False,
        "error_message" : "There is no matching BMP site name record for the Testsite record."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK -  CHECK - Each record in Testsite needs a corresponding record in BMPinfo, matched on sitename
    print("# END CHECK - CHECK - Each record in Testsite needs a corresponding record in BMPinfo, matched on sitename")

    # CHECK - Each record in BMPInfo needs corresponding record in TestSite, matched on sitename
    print("#  CHECK - Each record in BMPInfo needs corresponding record in TestSite, matched on sitename")
    badrows = mismatch(bmp, testsite, mergecols=['sitename'])
    args.update({
        "dataframe": bmp,
        "tablename": "tbl_bmpinfo",
        "badrows": badrows,
        "badcolumn": "sitename",
        "error_type" : "Logic Error",
        "is_core_error": False,
        "error_message" : "There is no matching test site name record for the BMPInfo record."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK -  CHECK - Each record in BMPInfo needs corresponding record in TestSite, matched on sitename
    print("# END CHECK - CHECK - Each record in BMPInfo needs corresponding record in TestSite, matched on sitename")





    # CHECK -Each record in Catchment needs corresponding record in BMPInfo, matched on sitename, bmpname
    print("#  CHECK - Each record in Catchment needs corresponding record in BMPInfo, matched on sitename, bmpname")
    badrows = mismatch(catchment, bmp, mergecols=['bmpname', 'sitename'])
    args.update({
        "dataframe": catchment,
        "tablename": "tbl_catchment",
        "badrows": badrows,
        "badcolumn": "bmpname,sitename",
        "error_type" : "Logic Error",
        "is_core_error": False,
        "error_message" : "There is no matching record in BMPinfo for the Catchment record, based on site name or bmp name."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - Each record in Catchment needs corresponding record in BMPInfo, matched on sitename, bmpname
    print("# END CHECK - Each record in Catchment needs corresponding record in BMPInfo, matched on sitename, bmpname")





    # CHECK -Each record in BMPInfo needs corresponding record in Catchment, matched on sitename, bmpname
    print("#  CHECK - Each record in BMPInfo needs corresponding record in Catchment, matched on sitename, bmpname")
    badrows = mismatch(bmp, catchment, mergecols=['sitename','bmpname'])
    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "sitename,bmpname",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "Warning: There is no matching record in Catchment for the BMPInfo record, based on site name and bmp name."
    })
    warnings = [*warnings, checkData(**args)]

    # END CHECK - Each record in BMPInfo needs corresponding record in Catchment, matched on sitename, bmpname
    print("# END CHECK - Each record in BMPInfo needs corresponding record in Catchment, matched on sitename, bmpname")





    # CHECK -Each record in BMPInfo needs a corresponding record in MonitoringStation, matched on sitename, bmpname
    print("#  CHECK - Each record in BMPInfo needs a corresponding record in MonitoringStation, matched on sitename, bmpname")
    badrows = mismatch(bmp, ms, mergecols=['sitename','bmpname'])
    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "sitename,bmpname",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no matching record in MonitoringStation for the BMPInfo record, based on site name and bmp name."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - Each record in BMPInfo needs a corresponding record in MonitoringStation, matched on sitename, bmpname
    print("# END CHECK -Each record in BMPInfo needs a corresponding record in MonitoringStation, matched on sitename, bmpname")




    # CHECK -Each record in MonitoringStation needs corresponding record in BMPInfo, matched on sitename, bmpname
    print("#  CHECK - Each record in MonitoringStation needs corresponding record in BMPInfo, matched on sitename, bmpname")
    badrows = mismatch(ms, bmp, mergecols=['sitename','bmpname'])
    args.update({
        "dataframe": ms,
        "tablename": 'tbl_monitoringstation',
        "badrows": badrows,
        "badcolumn": "sitename,bmpname",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no matching record in BMPInfo for the Monitoring Station record, based on site name and bmp name."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - Each record in MonitoringStation needs corresponding record in BMPInfo, matched on sitename, bmpname
    print("# END CHECK -Each record in MonitoringStation needs corresponding record in BMPInfo, matched on sitename, bmpname")



    # CHECK - A bypass monitoringstation may not be associated with a BMP with upstream treatment
    print("CHECK - A bypass monitoringstation may not be associated with a BMP with upstream treatment")
    unified_ms_bmp = pd.merge(ms, bmp, on='bmpname', how='inner', suffixes=('_ms', '_bmp'))


    #unified_ms_bmp = pd.read.sql("SELECT tbl_monitoringstation.stationtype, tbl_monitoringstation.bmpname, tbl_monitoringstation.sitename, tbl_bmpinfo.upstreamtreatmentispresent FROM tbl_monitoringstation INNER JOIN tbl_bmpinfo ON tbl_monitoringstation.bmpname = tbl_bmpinfo.bmpname", g.eng)
    badrows = unified_ms_bmp[(unified_ms_bmp['stationtype'].str.lower() == 'bypass') & (unified_ms_bmp['upstreamtreatmentispresent'].str.lower() == 'yes')].tmp_row_ms.tolist()   
    args.update({
        "dataframe": ms,
        "tablename": "tbl_monitoringstation",
        "badrows": badrows,
        "badcolumn": "stationtype,bmpname",
        "error_type" : "Logic Error",
        "error_message" :
            "This Monitoring Station is a bypass, " 
            "but it seems like it is associated with a BMP which has upstream treatment"
    })
    errs = [*errs, checkData(**args)]
    # END CHECK - A bypass monitoringstation may not be associated with a BMP with upstream treatment
    print("END CHECK - A bypass monitoringstation may not be associated with a BMP with upstream treatment")


    
    # CHECK - Each Testsite needs an associated monitoringstation with a measurement type of "P" (Each site needs a rain gauge)
    print("CHECK - Each Testsite needs an associated monitoringstation with a measurement type of 'p' (Each site needs a rain gauge)")
    
    test_mask = ms.measurementtype.apply(lambda x: any('P' in val.upper() for val in str(x).split(',')))
    #test_mask = ms.measurementtype.apply(lambda x: 'P' in [str(val).upper() for val in str(x).split(',')])
    #print(test_mask)

    badrows = mismatch(
        testsite, 
        ms[test_mask], 
        mergecols=['sitename']
    )
    args.update({
        "dataframe": testsite,
        "tablename": 'tbl_testsite',
        "badrows": badrows,
        "badcolumn": "sitename",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is no matching rain gauge collecting monitoring station record for the Testsite record."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - Each Testsite needs an associated monitoringstation with a measurement type of "P" (Each site needs a rain gauge)
    print("END CHECK - Each Testsite needs an associated monitoringstation with a measurement type of 'p' (Each site needs a rain gauge)")
    


    # CHECK - BMP Inflow count should match that listed in the Monitoring station tab (Warn since there may be inflow points that are not monitored)
    print("CHECK - BMP Inflow count should match that listed in the Monitoring station tab (Warn since there may be inflow points that are not monitored)")
    badrows = bmp[bmp.apply(lambda row: row['inflowcount'] != len(ms[ms['bmpname'] == row['bmpname']].reset_index(drop=True)), axis=1)].tmp_row.tolist()

    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "inflowcount",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "Warning: The BMP Inflow count does not match the number of listed monitoring stations."
    })
    warnings = [*warnings, checkData(**args)]
    # END CHECK - BMP Inflow count should match that listed in the Monitoring station tab (Warn since there may be inflow points that are not monitored)
    print("END CHECK - BMP Inflow count should match that listed in the Monitoring station tab (Warn since there may be inflow points that are not monitored)")




    # CHECK - Each BMP should have an "Influent" and "Effluent" monitoring station
    print("CHECK - Each BMP should have an 'Influent' and 'Effluent' monitoring station")

    # Each BMP has to have a monitoringstation with each of the "inflow" and "outflow" stationtypes
    # Meaning, at least one of each
    inflow_stationtypes = ['Influent','Reference Inflow']
    outflow_stationtypes = ['Effluent','Reference Outflow', 'Outflow + Overflow']
    
    # Merge BMP with MS and see what it has
    tmp = (
        bmp.merge(ms[['bmpname', 'stationname', 'stationtype']], on='bmpname', how='left')
        .groupby('bmpname')
        .agg(
            inflow_present=('stationtype', lambda x: x.isin(inflow_stationtypes).any()),
            outflow_present=('stationtype', lambda x: x.isin(outflow_stationtypes).any()),
            contributing_indices=('tmp_row', list)
        )
    )
    
    # list of lists of the "badrows"
    badrows = tmp[~(tmp.inflow_present & tmp.outflow_present)].contributing_indices.tolist()

    #print("badrows")
    #print(badrows)

    # flatten the list of lists
    badrows = sorted(set(item for sublist in badrows for item in sublist))

    #print("badrows")
    #print(badrows)

    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "bmpname",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "Warning: There is no matching 'Influent' or 'Effluent' monitoring station record for the BMP record."
    })
    warnings = [*warnings, checkData(**args)]

    # END CHECK - Each BMP should have an "Influent" and "Effluent" monitoring station
    print("END CHECK - Each BMP should have an 'Influent' and 'Effluent' monitoring station")



    ##############################################################################################################################
    # -----------------------------                         END Logic Checks                                    ----------------------- #
    ##############################################################################################################################
    print("# ----------------------                         END Logic Checks                                    ----------------------- #")
    
    

    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- # 





    print("# ----------------------                         TestSite Checks                                    ----------------------- #")
    ######################################################################################################################################
    # -----------------------------                         TestSite Checks                                    --------------------------- #
    ######################################################################################################################################


    # CHECK - NumberofBMPs should match the actual number of BMPs in the BMPInfo tab (Making it a warning in case they have some that they do not monitor, or 
    print("NumberofBMPs should match the actual number of BMPs in the BMPInfo tab (Making it a warning in case they have some that they do not monitor, or something like that.)")
    badrows = testsite[testsite.apply(lambda row: row['numberofbmps'] != len(bmp[bmp['sitename'] == row['sitename']].reset_index(drop=True)), axis=1)].tmp_row.tolist()
    
    
    args.update({
        "dataframe": testsite,
        "tablename": 'tbl_testsite',
        "badrows": badrows,
        "badcolumn": "numberofbmps",
        "error_type": "Logic Warning",
        "is_core_error": False,
        "error_message": "Warning: NumberofBMPs in tbl_testsite does not match the actual number of BMPs in tbl_bmpinfo."
    })
    warnings = [*warnings, checkData(**args)]

    # END CHECK - NumberofBMPs should match the actual number of BMPs in the BMPInfo tab (Making it a warning in case they have some that they do not monitor, or something like that.)
    print("END CHECK - NumberofBMPs should match the actual number of BMPs in the BMPInfo tab (Making it a warning in case they have some that they do not monitor, or something like that.)")




    # Check - If NumberofBMPS does not match the number of BMPs in the submissionm, a commment is required.
    print("CHECK - If NumberofBMPS does not match the number of BMPs in the submission, a comment is required.")
    testsite_bmp = testsite[testsite.apply(lambda row: row['numberofbmps'] != len(bmp[bmp['sitename'] == row['sitename']].reset_index(drop=True)), axis=1)]
    
    badrows = testsite_bmp[testsite_bmp['comments'].isnull()].tmp_row.tolist()
    
    args.update({
        "dataframe": testsite,
        "tablename": "tbl_testsite",
        "badrows": badrows,
        "badcolumn": "comments",
        "error_type" : "Logic Error",
        "error_message" : "There is no comment provided where the NumberofBMPS in tbl_testsite does not match the actual number of BMPs."
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - If NumberofBMPS does not match the number of BMPs in the submission, a commment is required.
    print("END CHECK - If NumberofBMPS does not match the number of BMPs in the submission, a commment is required.")





    # CHECK - Zipcode should be a 5 digit positive number ( in actuality, it should follow a regex pattern \d{5} because it can start with a zero)
    print("CHECK - Zipcode should be a 5 digit positive number ( in actuality, it should follow a regex pattern \d{5} because it can start with a zero)")
    # Ensure zipcode is exactly 5 digits (implicitly positive)
    badrows = testsite[~testsite['zipcode'].astype(str).str.match(r'^\d{5}$')].tmp_row.tolist()

    # In excel, 0 is not being read as a number, so it is being read as a string. This is why we need to convert it to a string

    args.update({
        "dataframe": testsite,
        "tablename": "tbl_testsite",
        "badrows": badrows,
        "badcolumn": "zipcode",
        "error_type" : "Logic Error",
        "error_message" : "Zipcodes must be a five-digit positive number."
    })
    errs = [*errs, checkData(**args)]

    #END CHECK - Zipcode should be a 5 digit positive number ( in actuality, it should follow a regex pattern \d{5} because it can start with a zero)
    print("END CHECK - Zipcode should be a 5 digit positive number ( in actuality, it should follow a regex pattern \d{5} because it can start with a zero)")





    ##############################################################################################################################
    # -----------------------------                         END TestSite Checks                                    ----------------------- #
    ##############################################################################################################################
    print("# ----------------------                         END Testsite Checks                                    ----------------------- #")
    
    

    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- # 





    print("# ----------------------                         BMP Info Checks                                    ----------------------- #")
    ######################################################################################################################################
    # -----------------------------                         BMP Info Checks                                    --------------------------- #
    ######################################################################################################################################



    # CHECK - BMP Names can't have commas in them
    print("CHECK - BMP Names can't have commas in them")
    badrows = bmp[bmp.bmpname.apply(lambda x: ',' in str(x))].tmp_row.tolist()


    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "bmpname",
        "error_type": "Format Error",
        "is_core_error": False,
        "error_message": "Bmp names cannot have commas in them."
    })

    errs = [*errs, checkData(**args)]

    # END CHECK - BMP Names can't have commas in them
    print("END CHECK - BMP Names can't have commas in them")




    # CHECK - UpstreamBMPNames must be found in the list of BMP's that are being submitted
    print("CHECK - UpstreamBMPNames must be found in the list of BMP's that are being submitted.")
    #Create set with valid BMP names from the column bmpname
    valid_bmp_names = set(bmp['bmpname'].dropna().values)
    print(valid_bmp_names)

    # Find rows in bmp where upstreambmpnames is not in the valid_bmp_names list
    badrows = bmp[
        (bmp['upstreamtreatmentispresent'].str.lower() == 'yes') & 
         (bmp['upstreambmpnames'].apply(lambda x: any(name.strip() not in valid_bmp_names for name in x.split(',')) if pd.notna(x) else True))
    ].tmp_row.tolist()


    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "upstreambmpnames, upstreamtreatmentispresent",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "There is an UpstreamBMPName that is not found in the list of BMP's that are being submitted."
    })

    errs = [*errs, checkData(**args)]

    # END CHECK - UpstreamBMPNames must be found in the list of BMP's that are being submitted
    print("END CHECK - UpstreamBMPNames must be found in the list of BMP's that are being submitted.")




    # CHECK - If "UpstreamTreatmentIsPresent" is set to "yes," then "UpstreamBMPName" must also be provided.
    print("CHECK - If 'UpstreamTreatmentIsPresent' is set to 'yes,' then 'UpstreamBMPName' must also be provided.")
    badrows = bmp[(bmp['upstreamtreatmentispresent'].str.lower() == 'yes') & (bmp['upstreambmpnames'].isnull())].tmp_row.tolist()

    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "upstreambmpnames",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "BMP with present upstream treatment must have a corresponding UpstreamBMPName."
    })

    errs = [*errs, checkData(**args)]
    # END CHECK - If "UpstreamTreatmentIsPresent" is set to "yes," then "UpstreamBMPName" must also be provided.
    print("END CHECK - If 'UpstreamTreatmentIsPresent' is set to 'yes,' then 'UpstreamBMPName' must also be provided.")





    # CHECK - if designstormdepth is less than zero, it must be -88
    print("# CHECK - if designstormdepth is less than zero, it must be -88")

    badrows = bmp[
        (bmp['designstormdepth'] < 0) & (bmp['designstormdepth'] != -88)
    ].tmp_row.tolist()
    
    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "designstormdepth",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If designstormdepth is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if designstormdepth is less than zero, it must be -88
    print("# END CHECK - if designstormdepth is less than zero, it must be -88")
    




    # CHECK - if designcapturevolume is less than zero, it must be -88
    print("# CHECK - if designcapturevolume is less than zero, it must be -88")

    badrows = bmp[
        (bmp['designcapturevolume'] < 0) & (bmp['designcapturevolume'] != -88)
    ].tmp_row.tolist()

    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "designcapturevolume",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If designcapturevolume is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if designcapturevolume is less than zero, it must be -88
    print("# END CHECK - if designcapturevolume is less than zero, it must be -88")




    # CHECK - if designflowrate is less than zero, it must be -88
    print("# CHECK - if designflowrate is less than zero, it must be -88")

    badrows = bmp[
        (bmp['designflowrate'] < 0) & (bmp['designflowrate'] != -88)
    ].tmp_row.tolist()

    args.update({
        "dataframe": bmp,
        "tablename": 'tbl_bmpinfo',
        "badrows": badrows,
        "badcolumn": "designflowrate",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If designflowrate is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if designflowrate is less than zero, it must be -88
    print("# END CHECK - if designflowrate is less than zero, it must be -88")




    print("# ----------------------                         BMP Info Checks                                    ----------------------- #")
    ######################################################################################################################################
    # -----------------------------                         BMP Info Checks                                    --------------------------- #
    ######################################################################################################################################



    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- # 




    ##############################################################################################################################
    # -----------------------------                         Monitoring Station Checks                            ----------------------- #
    ##############################################################################################################################
    print("# ----------------------                         Monitoring Station Checks                            ----------------------- #")
    

    # Check - Measurement Types (which are entered, separated by commas) must be found in lu_measurementtype
    print("Check - Measurement Types (which are entered, separated by commas) must be found in lu_measurementtype")
    
     # Query the lu_measurementtype table to get valid measurement type values
    valid_measurementtype = pd.read_sql("SELECT measurementtype FROM lu_measurementtype", eng)['measurementtype'].tolist()

    # Find rows in ms where any qacode is not in the valid_qacodes list
    badrows = ms[ms['measurementtype'].apply(lambda x: any(code.strip() not in valid_measurementtype for code in str(x).split(',')))].tmp_row.tolist()

    args.update({
        "dataframe": ms,
        "tablename": 'tbl_monitoringstation',
        "badrows": badrows,
        "badcolumn": "measurementtype",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Invalid Measurement type. It must be found in the lu_measurementtype table."
    })
    errs = [*errs, checkData(**args)]


    # END CHECK - Measurement Types (which are entered, separated by commas) must be found in lu_measurementtype
    print("END Check - Measurement Types (which are entered, separated by commas) must be found in lu_measurementtype")




    ##############################################################################################################################
    # -----------------------------                         END Monitoring Station Checks                         ----------------------- #
    ##############################################################################################################################
    print("# ----------------------                         END Monitoring Station Checks                         ----------------------- #")
    


    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- # 




    print("# ----------------------                         Catchment Checks                                       ----------------------- #")
    ######################################################################################################################################
    # -----------------------------                         Catchment Checks                                       --------------------------- #
    ######################################################################################################################################

    catchment 


    # Check - If insitusoilmeasuretype is NRCS then the insitusoilvalue must not be N/A
    print("Check - If insitusoilmeasuretype is NRCS then the insitusoilvalue must not be N/A")

    badrows = catchment[
        (catchment['insitusoilmeasuretype'] == 'NRCS') & (catchment['insitusoilvalue'] == 'N/A')
    ].tmp_row.tolist()

    #print(badrows)

    args.update({
        "dataframe": catchment,
        "tablename": 'tbl_catchment',
        "badrows": badrows,
        "badcolumn": "insitusoilmeasuretype,insitusoilvalue",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If insitusoilmeasuretype is NRCS then the insitusoilvalue must not be N/A. "
    })
    errs = [*errs, checkData(**args)]
  

    # END CHECK - If insitusoilmeasuretype is NRCS then the insitusoilvalue must not be N/A
    print("END Check - If insitusoilmeasuretype is NRCS then the insitusoilvalue must not be N/A.")




    # Check - If insitusoilmeasuretype is NRCS then the  insitusoilunits must not be N/A
    print("Check - If insitusoilmeasuretype is NRCS then the  insitusoilunits must not be N/A")

    badrows = catchment[
        (catchment['insitusoilmeasuretype'] == 'NRCS') & (catchment['insitusoilunits'] == 'N/A')
    ].tmp_row.tolist()

    #print(badrows)
    
    args.update({
        "dataframe": catchment,
        "tablename": 'tbl_catchment',
        "badrows": badrows,
        "badcolumn": "insitusoilmeasuretype,insitusoilunits",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If insitusoilmeasuretype is NRCS then the  insitusoilunits must not be N/A. "
    })
    errs = [*errs, checkData(**args)]
  
    # END CHECK - If insitusoilmeasuretype is NRCS then the  insitusoilunits must not be N/A
    print("END Check - If insitusoilmeasuretype is NRCS then the insitusoilunits must not be N/A.")






    # CHECK - if catchmentarea is less than zero, it must be -88
    print("# CHECK - if catchmentarea is less than zero, it must be -88")

    badrows = catchment[
        (catchment['catchmentarea'] < 0) & (catchment['catchmentarea'] != -88)
    ].tmp_row.tolist()

    args.update({
        "dataframe": catchment,
        "tablename": 'tbl_catchment',
        "badrows": badrows,
        "badcolumn": "catchmentarea",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "If catchmentarea is less than zero, it must be -88"
    })
    errs = [*errs, checkData(**args)]

    # END CHECK - if catchmentarea is less than zero, it must be -88
    print("# END CHECK - if catchmentarea is less than zero, it must be -88")




    # CHECK - sum of drainagepctlanduse equal 100 (group by sitename, bmpname, drainageareacharacterization)
    print("# CHECK - sum of drainagepctlanduse must equal 100 (group by sitename, bmpname, drainageareacharacterization)")
    #grouped = catchment.groupby(['sitename', 'bmpname', 'drainageareacharacterization'])['drainagepctlanduse'].sum()
   
    #results = grouped[grouped != 100]
    #results = results.index.tolist()

    # grouped = catchment.groupby(['sitename', 'bmpname', 'drainageareacharacterization'])['drainagepctlanduse'].sum()
    grouped = catchment \
        .groupby(['sitename', 'bmpname']) \
        .agg(
            drainageusepctsum = ('drainagepctlanduse', 'sum'),
            rows = ('tmp_row', list)
        )

    if not grouped.empty:
        badrows = grouped[grouped.drainageusepctsum.apply(lambda x: round(x, 0)) != 100].rows.tolist()


        print("grouped")
        print(grouped)

        

        # Next we flatted the list of lists
        badrows = sorted(set(item for sublist in badrows for item in sublist))

        args.update({
            "dataframe": catchment,
            "tablename": 'tbl_catchment',
            "badrows": badrows,
            "badcolumn": "drainageareacharacterization",
            "error_type": "Value Error",
            "is_core_error": False,
            "error_message": "The sum of drainageptlanduse must equal to 100."
        })
        errs = [*errs, checkData(**args)]

    # END CHECK - sum of drainagepctlanduse must equal 100 (group by sitename, bmpname, drainageareacharacterization)
    print("END CHECK - sum of drainagepctlanduse must equal 100 (group by sitename, bmpname, drainageareacharacterization)")


    ##############################################################################################################################



    print("you are doing a great job :)")


    errs = [e for e in errs if len(e) > 0]
    warnings = [w for w in warnings if len(w) > 0]
    print("Done with custom checks")
    return {'errors': errs, 'warnings': warnings}
