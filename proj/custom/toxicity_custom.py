# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g, session
from .functions import checkData, multivalue_lookup_check, sample_assignment_check, mismatch
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import copy
from scipy import stats
import os

def toxicity(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    eng = g.eng

    # define errors and warnings list
    errs = []
    warnings = []
   
        
    toxbatch = all_dfs['tbl_toxbatch']
    toxbatch = toxbatch.assign(tmp_row = toxbatch.index)
    
    toxresults = all_dfs['tbl_toxresults']
    toxresults = toxresults.assign(tmp_row = toxresults.index)
    
    toxwq = all_dfs['tbl_toxwq']
    toxwq = toxwq.assign(tmp_row = toxwq.index)
    

    toxbatch_args = {
        "dataframe": toxbatch,
        "tablename": 'tbl_toxbatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxresults_args = {
        "dataframe": toxresults,
        "tablename": 'tbl_toxresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    toxwq_args = {
        "dataframe": toxwq,
        "tablename": 'tbl_toxwq',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    

    ## LOGIC ##
    print("Starting Toxicity Logic Checks")
    
    # 1 - All records for each table must have a corresponding record in the other tables due on submission. Join tables on Agency/LabCode and ToxBatch/QABatch
    ### make sure there are records that match between batch and result - otherwise big problem
    # EACH TAB MUST HAVE A CORRESPONDING RELATED RECORD IN ALL THE OTHER TABS - JOIN TABLES BASED ON TOXBATCH AND LAB
    
    # batch
    # Get rows in Batch but not in Results
    # Relating on toxbatch, lab, matrix, and species ensures that they dont put the wrong species with a toxbatch identifier
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxbatch[~toxbatch[['toxbatch','lab','matrix','species']].isin(toxresults[['toxbatch','lab','matrix','species']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxbatch, toxresults, ['toxbatch','lab','matrix','species'])
    toxbatch_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity Batch record must have a corresponding Toxicity Result record. Records are matched on ToxBatch, Lab, Matrix and Species"
    })
    errs = [*errs, checkData(**toxbatch_args)]
    
    # Batch and WQ are related based on toxbatch and lab
    # Get rows in Batch but not in WQ
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxbatch[~toxbatch[['toxbatch','lab']].isin(toxwq[['toxbatch','lab']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxbatch, toxwq, ['toxbatch','lab'])
    toxbatch_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity Batch record must have a corresponding Toxicity WQ record. Records are matched on ToxBatch and Lab."
    })
    errs = [*errs, checkData(**toxbatch_args)]

    
    # result
    # Result and batch are related on toxbatch, lab, matrix and species
    # Get rows in Results but not in Batch
    # Relating on toxbatch, lab, matrix, and species ensures that they dont put the wrong species with a toxbatch identifier
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxresults[~toxresults[['toxbatch','lab','matrix','species']].isin(toxbatch[['toxbatch','lab','matrix','species']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxresults, toxbatch, ['toxbatch','lab','matrix','species'])
    toxresults_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab,matrix,species",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity Results record must have a corresponding Toxicity Batch record. Records are matched on ToxBatch, Lab, Matrix and Species"
    })
    errs = [*errs, checkData(**toxresults_args)]

    # Result and wq are related on stationid, toxbatch, lab
    # Get rows in Results but not in WQ
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxresults[~toxresults[['stationid','toxbatch','lab']].isin(toxwq[['stationid','toxbatch','lab']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxresults, toxwq, ['stationid','toxbatch','lab'])
    toxresults_args.update({
        "badrows": badrows,
        "badcolumn": "stationid,toxbatch,lab",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity Result record must have a corresponding Toxicity WQ record. Records are matched on StationID, ToxBatch and Lab."
    })
    errs = [*errs, checkData(**toxresults_args)]

    # wq
    # Batch and WQ are related based on toxbatch and lab
    # Get rows in WQ but not in Batch
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxwq[~toxwq[['toxbatch','lab']].isin(toxbatch[['toxbatch',    'lab']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxwq, toxbatch, ['toxbatch','lab'])
    toxwq_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity WQ record must have a corresponding Toxicity Batch record. Records are matched on ToxBatch and Lab."
    })
    errs = [*errs, checkData(**toxwq_args)]
    
    # Result and wq are related on stationid, toxbatch, lab
    # Get rows in WQ but not in Results
    # This relationship between tables was verified by Darrin on 7/19/2023
    # badrows = toxwq[~toxwq[['stationid','toxbatch','lab']].isin(toxresults[['stationid','toxbatch','lab']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    badrows = mismatch(toxwq, toxresults, ['stationid','toxbatch','lab'])
    toxwq_args.update({
        "badrows": badrows,
        "badcolumn": "stationid,toxbatch,lab",
        "error_type": "Logic Error",
        "error_message": "Each Toxicity WQ record must have a corresponding Toxicity Result record. Records are matched on StationID, ToxBatch and Lab."
    })
    errs = [*errs, checkData(**toxwq_args)]



    # 2 - Check for the minimum number of replicates - ee = 4 and mg = 5 and na = 10
    ## first get a lab replicate count grouped on stationid, toxbatch, species, and sampletypecode
    dfrep = pd.DataFrame(toxresults.groupby(['stationid','toxbatch','species','sampletypecode']).size().reset_index(name='replicatecount'))
    
    ## merge the lab replicant group with results so that you can get the tmp_row - the lab rep count will be matched with each lab rep
    ## we will want to highlight them as a group rather than by row
    dfrep = pd.merge(dfrep,toxresults, on=['stationid','toxbatch','species','sampletypecode'], how='inner')

    ## A MINIMUM NUMBER OF 4 REPLICATES ARE REQUIRED FOR SPECIES EOHAUSTORIUS ESTUARIUS  (Reference Toxicant)##
    print("## A MINIMUM NUMBER OF 4 REPLICATES ARE REQUIRED FOR SPECIES EOHAUSTORIUS ESTUARIUS (Reference Toxicant) ##")
    badrows = dfrep[
        (dfrep['sampletypecode'].isin(['RFNH3'])) & 
        (dfrep['species'].isin(['Eohaustorius estuarius'])) & 
        (dfrep['replicatecount'] < 4)
    ].tmp_row.tolist()
    toxresults_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab,sampletypecode",
        "error_type": "Logic Error",
        "error_message": "A minimum number of 4 replicates is required for species Eohaustorius estuarius with any of the following SampleTypeCode: RFNH3."
    })
    errs = [*errs, checkData(**toxresults_args)] 


    ## A MINIMUM NUMBER OF 5 REPLICATES ARE REQUIRED FOR SPECIES EOHAUSTORIUS ESTUARIUS ##
    print("## A MINIMUM NUMBER OF 5 REPLICATES ARE REQUIRED FOR SPECIES EOHAUSTORIUS ESTUARIUS ##")
    badrows = dfrep[
        (dfrep['sampletypecode'].isin(['CNEG', 'CNSL', 'Grab', 'QA'])) & 
        (dfrep['species'].isin(['Eohaustorius estuarius'])) & 
        (dfrep['replicatecount'] < 5)
    ].tmp_row.tolist()
    toxresults_args.update({
        "badrows": badrows,
        "badcolumn": "toxbatch,lab,sampletypecode",
        "error_type": "Logic Error",
        "error_message": "A minimum number of 5 replicates is required for species Eohaustorius estuarius with any of the following SampleTypeCode: CNEG, CNSL, Grab, QA."
    })
    errs = [*errs, checkData(**toxresults_args)] 

    ## A MINIMUM NUMBER OF 5 REPLICATES ARE REQUIRED FOR SPECIES MYTILUS GALLOPROVINCIALIS ##
    print("## A MINIMUM NUMBER OF 5 REPLICATES ARE REQUIRED FOR SPECIES MYTILUS GALLOPROVINCIALIS ##")
    badrows = dfrep[
        (dfrep['species'].isin(['Mytilus galloprovincialis','MG'])) & 
        (dfrep['replicatecount'] < 5)
    ].tmp_row.tolist()
    toxresults_args.update({
        "dataframe": toxresults,
        "tablename": 'tbl_toxresults',
        "badrows": badrows,
        "badcolumn": "toxbatch,lab,sampletypecode",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "A minimum number of 5 replicates is required for species Mytilus galloprovincialis"
    })
    errs = [*errs, checkData(**toxresults_args)] 


    # This was only used in Bight23 Toxicity intercalibration
    ## A MINIMUM NUMBER OF 10 REPLICATES ARE REQUIRED FOR SPECIES NEANTHES ARENACEODENTATA ##
    # print("## A MINIMUM NUMBER OF 10 REPLICATES ARE REQUIRED FOR SPECIES NEANTHES ARENACEODENTATA ##")
    
    # badrows = dfrep[
    #     (dfrep['sampletypecode'].isin(['CNEG','CNSL','Grab'])) & 
    #     (dfrep['species'].isin(['Neanthes arenaceodentata'])) & 
    #     (dfrep['replicatecount'] < 10)
    # ].tmp_row.tolist()
    # toxresults_args.update({
    #     "dataframe": toxresults,
    #     "tablename": 'tbl_toxresults',
    #     "badrows": badrows,
    #     "badcolumn": "toxbatch,lab,sampletypecode",
    #     "error_type": "Logic Error",
    #     "is_core_error": False,
    #     "error_message": "A minimum number of 10 replicates is required for species Neanthes arenaceodentata with any of the following SampleTypeCode: CNEG, CNSL, Grab."
    # })
    # errs = [*errs, checkData(**toxresults_args)]
    


    # 3. EACH BS or SWI BATCH MUST HAVE A "REFERENCE TOXICANT" BATCH WITHIN A SPECIFIED DATE RANGE.
    print("# 3. EACH BS or SWI BATCH MUST HAVE A REFERENCE TOXICANT BATCH WITHIN A SPECIFIED DATE RANGE.")
    # get reference toxicant dataframe
    batchrt = toxbatch[
        ['toxbatch', 'teststartdate', 'actualtestduration', 'actualtestdurationunits', 'referencebatch']
    ].where(toxbatch['matrix'].isin(['RT','Reference Toxicant']))
    # drop emptys
    batchrt = batchrt.dropna()
    if len(batchrt.index) != 0:
        # get bs dataframe added swi on 21june17
        batchbs = toxbatch[
            ['toxbatch', 'matrix', 'species', 'teststartdate', 'actualtestduration', 'actualtestdurationunits', 'referencebatch','tmp_row']
        ].where(toxbatch['matrix'].isin(['BS','SWI','Whole Sediment','Sediment Water Interface']))
        # drop emptys
        batchbs = batchbs.dropna()
        # get bs dataframe
        if len(batchbs.index) != 0:
            # find any bs batch records with a missing rt 
            # merge bs and rt
            bsmerge = pd.merge(batchbs, batchrt, how = 'inner', on = ['referencebatch'])
            if len(bsmerge.index) != 0:
                # create date range column
                def checkRTDate(grp):
                    grp['teststartdate_x'] = pd.to_datetime(grp['teststartdate_x'])
                    grp['teststartdate_y'] = pd.to_datetime(grp['teststartdate_y'])
                    d = grp['teststartdate_x'] - grp['teststartdate_y']
                    grp['daterange'] = abs(d.days)
                    return grp
                bsmerge = bsmerge.apply(checkRTDate, axis = 1)
                # checks by species and datarange
                badrows = bsmerge.loc[(bsmerge['species'] == 'Eohaustorius estuarius') & (bsmerge['daterange'] > 10)].tmp_row.tolist()
                toxbatch_args.update({
                    "dataframe": toxbatch,
                    "tablename": 'tbl_toxbatch',
                    "badrows": badrows,
                    "badcolumn": "matrix",
                    "error_type": "Logic Error",
                    "is_core_error": False,
                    "error_message": "Each Whole Sediment or Sediment Water Interface batch must have a Reference Toxicant batch that starts within a specified time period: EE less than 10 days"
                })
                errs = [*errs, checkData(**toxbatch_args)] 
                
                badrows = bsmerge.loc[
                    (bsmerge['species'] == 'Mytilus galloprovincialis') & (bsmerge['daterange'] > 2)
                ].tmp_row.tolist()
                toxbatch_args.update({
                    "dataframe": toxbatch,
                    "tablename": 'tbl_toxbatch',
                    "badrows": badrows,
                    "badcolumn": "matrix",
                    "error_type": "Logic Error",
                    "is_core_error": False,
                    "error_message": "Each Whole Sediment or Sediment Water Interface batch must have a Reference Toxicant batch that starts within a specified time period: MG less than 2 days"
                })
                errs = [*errs, checkData(**toxbatch_args)] 
        else:
            toxbatch_args.update({
                    "dataframe": toxbatch,
                    "tablename": 'tbl_toxbatch',
                    "badrows": toxbatch.tmp_row.to_list(),
                    "badcolumn": "matrix",
                    "error_type": "Logic Error",
                    "is_core_error": False,
                    "error_message": "A submission requires a Bulk Sediment record in batch submission"
                })
            errs = [*errs, checkData(**toxbatch_args)] 
    else:
        toxbatch_args.update({
            "dataframe": toxbatch,
            "tablename": 'tbl_toxbatch',
            "badrows": toxbatch.tmp_row.to_list(),
            "badcolumn": "matrix",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "A submission requires a Reference Toxicant record in batch submission"
        })
        errs = [*errs, checkData(**toxbatch_args)] 


    #######################################################################
    # ------------ Check for previously submitted field data ------------ #
    #######################################################################
    print("# Eric - A toxicity submission (batch, results, wq) requires that the field data be submitted first. ")
    print("# To check all unique Result/StationID records should have a corresponding record in Field/Grab/StationID (make sure it wasn't abandoned also). This should be an error.")
    
    # first we need to call the field grab event table and get back all the stations that did not fail
    sql_df = pd.read_sql("SELECT stationid FROM tbl_grabevent WHERE toxicity = 'Yes' AND grabfail = 'None or No Failure'", eng)
    
    # get only the unique records for database/stationid
    unique_stations = sql_df.stationid.unique()

    # find what records dont match the unique stations in the database
    badrows = toxresults[ ( ~toxresults.stationid.isin(unique_stations) ) & (toxresults.sampletypecode.isin(['Grab','QA']) ) ].tmp_row.tolist()
    toxresults_args.update({
        "badrows": badrows,
        "badcolumn": "stationid",
        "error_type": "Undefined Error",
        "error_message": "A toxicity submission requires that the field data be submitted first. Your station does not match the grab event table."
    })

    
    # Changed to a warning on 10/10/2023 per Darrin's request
    # Submitted data will have field info missing from the tox summary
    # We will delete those submissions, make this an error again, and have them resubmit

    # Changed back to an error on 12/5/2023 per Darrin's request - Most field data has been submitted by this point
    errs = [*errs, checkData(**toxresults_args)]

    ###########################################################################
    # ------------ END Check for previously submitted field data ------------ #
    ###########################################################################





    # 4. LABREP IN RESULTS TAB NEEDS TO BE CONTIGUOUS.
    # MIGHT NEED TO ADD FIELDREPLICATE TO GROUP RESULTS
    print("4. LABREP IN RESULTS TAB NEEDS TO BE CONTIGUOUS.")
    grouping_cols = [
        'stationid',
        'toxbatch',
        'matrix',
        'lab',
        'species',
        'dilution',
        'treatment',
        'concentration',
        'concentrationunits',
        'endpoint',
        'sampletypecode',
        'samplecollectdate',
        'fieldreplicate'
    ]
    #dflabrep = toxresults.groupby(grouping_cols)
    #dflabrep = toxresults.groupby(grouping_cols)['labrep', 'tmp_row'].apply(lambda x: tuple([x.labrep.tolist(), x.tmp_row.tolist()]))
    dflabrep = toxresults.groupby(grouping_cols)[['labrep', 'tmp_row']].agg({'labrep':list,'tmp_row':list})
    dflabrep = dflabrep.reset_index()
    dflabrep.rename(columns = {'labrep':'reps', 'tmp_row':'rows'}, inplace = True)
    # dflabrep['reps'] = dflabrep.repsandrows.apply(lambda x: x[0]) # first value of the tuple is the labreplicate
    # dflabrep['rows'] = dflabrep.repsandrows.apply(lambda x: x[1]) # second value of the tuple is the row index number (tmp_row)
    #dflabrep.drop('repsandrows', axis=1, inplace=True)
    # checking to see if LabReplicated were labeled correctely
    # This is done using the sum of the first 'n' formula 1 + 2 + ... + n == n(n+1)/2
    dflabrep['passed'] = dflabrep['reps'].apply(lambda x: (sum(x) == ((len(x) * (len(x) + 1)) / 2 )) & (all([num > 0 for num in x])  ))
    badrows = [item for sublist in dflabrep[~dflabrep.passed].rows.tolist() for item in sublist]
    toxresults_args.update({
        "dataframe": toxresults,
        "tablename": 'tbl_toxresults',
        "badrows": badrows,
        "badcolumn": "labrep",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "LabReplicates must be contiguous."
    })
    errs = [*errs, checkData(**toxresults_args)] 
    print("Done with labrep check")


    ## END LOGIC CHECKS ##
    print("## END LOGIC CHECKS ##")

    # clean up the errors list
    errs = [e for e in errs if len(e) > 0]

    ## CUSTOM CHECKS ##
    print("## CUSTOM CHECKS ##")
    if len(errs) == 0:
        ## BATCH CHECKS ##
        print("Starting Toxicity Batch Information Checks")
        # 1. EACH BATCH WITH A MATRIX OF BS MUST INCLUDE A CORRESPONDING RESULT CNEG SAMPLE
        print("## EACH BATCH WITH A MATRIX OF 'Whole Sediment' or 'Sediment Water Interface' MUST INCLUDE A CORRESPONDING RESULT CNEG SAMPLE ##")
        # first get unique cneg records from result dataframe
        bsresult = toxresults[['toxbatch','sampletypecode']].where(toxresults['sampletypecode'] == 'CNEG')
        bsresult = bsresult.dropna() 
        bsresult['unique'] = np.nan
        bsresult = bsresult.groupby(['toxbatch','sampletypecode'])['unique'].nunique().reset_index()
        # second get unique batch records with a matrix of bs
        bsbatch = toxbatch[['toxbatch','matrix','tmp_row']].where(toxbatch['matrix'].isin(["Whole Sediment", "Sediment Water Interface"]))
        bsbatch = bsbatch.dropna()
        bsbatch['unique'] = np.nan
        bsbatch = bsbatch.groupby(['toxbatch','matrix','tmp_row'])['unique'].nunique().reset_index()
        # merge unique cneg and toxbatch records on where they match
        bsmerge = bsbatch.merge(bsresult, on='toxbatch', how='inner')
        bslocate = bsbatch[(~bsbatch.toxbatch.isin(bsmerge.toxbatch))].toxbatch.tolist()
        # label toxbatch records
        print(bsbatch[(~bsbatch.toxbatch.isin(bsmerge.toxbatch))])
        badrows = bsbatch[(~bsbatch.toxbatch.isin(bsmerge.toxbatch))].tmp_row.tolist()
        toxbatch_args.update({
            "badrows": badrows,
            "badcolumn": "matrix",
            "error_type": "Logic Error",
            "error_message": "Each batch with a matrix of 'Whole Sediment' or 'Sediment Water Interface' must include a corresponding toxresults CNEG sample."
        })
        errs = [*errs, checkData(**toxbatch_args)]  

        # 2. EACH BATCH WITH A MATRIX OF RT MUST INCLUDE A CORRESPONDING toxresults WITH SAMPLETYPECODE = RFNH3.
        print("## EACH BATCH WITH A MATRIX OF RT MUST INCLUDE A CORRESPONDING toxresults WITH SAMPLETYPECODE = RFNH3. ##")
        # first get unique rfnh3 records from result dataframe
        rtresult = toxresults[['toxbatch','sampletypecode']].where(toxresults['sampletypecode'] == 'RFNH3')
        rtresult = rtresult.dropna() 
        rtresult['unique'] = np.nan
        rtresult = rtresult.groupby(['toxbatch','sampletypecode'])['unique'].nunique().reset_index()
        # second get unique toxbatch records with a matrix of rt
        rtbatch = toxbatch[['toxbatch','matrix','tmp_row']].where(toxbatch['matrix'].isin(["Reference Toxicant","RT"]))
        rtbatch = rtbatch.dropna()
        rtbatch['unique'] = np.nan
        rtbatch = rtbatch.groupby(['toxbatch','matrix','tmp_row'])['unique'].nunique().reset_index()
        # merge unique rt and batch records on where they match
        rtmerge = rtbatch.merge(rtresult, on='toxbatch', how='inner')
        print(rtbatch[(~rtbatch.toxbatch.isin(rtmerge.toxbatch))])
        badrows = rtbatch[(~rtbatch.toxbatch.isin(rtmerge.toxbatch))].tmp_row.tolist()
        toxbatch_args.update({
            "badrows": badrows,
            "badcolumn": "matrix",
            "error_type": "Logic Error",
            "error_message": "Each batch with a matrix of RT must include a corresponding result SampleTypeCode = RFNH3."
        })
        errs = [*errs, checkData(**toxbatch_args)]      
        
        # 3. TESTACCEPTABILITY CHECK - A SINGLE QACODE IS REQUIRED BUT MULTIPLE QACODES ARE POSSIBLE (MANY TO MANY) author - Jordan Golemo
        ## WORKS BUT TOO SLOW
        print("TESTACCEPTABILITY CHECK - A SINGLE QACODE IS REQUIRED BUT MULTIPLE QACODES ARE POSSIBLE (MANY TO MANY)")
        tmpargs = multivalue_lookup_check(toxbatch, 'testacceptability', 'lu_toxtestacceptability', 'testacceptability', dbconnection = eng, displayfieldname = "TestAcceptability")
        toxbatch_args.update(tmpargs)
        errs = [*errs, checkData(**toxbatch_args)]

        # 4. ACTUAL TEST DURATION FOR EACH SPECIES IN BATCH TAB
        print("## ACTUAL TEST DURATION FOR EACH SPECIES IN BATCH TAB ##")
        # The Test duration for Eohaustorius estuarius (Regardless of matrix) should be either about 4 days, or 10 days. Darrin gave a buffer of 4 hours
        badrows = toxbatch[
                (toxbatch['species'] == 'Eohaustorius estuarius') 
                & (toxbatch['matrix'] == 'Reference Toxicant') 
                & 
                (
                    (
                        # ActualTestDurationUnits can only can be "Hours" or "Days" - it is tied to a lookup list lu_toxtestunits
                        ~toxbatch.apply(lambda row: row["actualtestduration"] if row["actualtestdurationunits"] == 'Hours' else row["actualtestduration"] * 24, 
                            axis = 1
                        ) \
                        .between(92, 100) # 96 hours +/- 4 hours
                    ) & (
                        # ActualTestDurationUnits can only can be "Hours" or "Days" - it is tied to a lookup list lu_toxtestunits
                        ~toxbatch.apply(lambda row: row["actualtestduration"] if row["actualtestdurationunits"] == 'Hours' else row["actualtestduration"] * 24, 
                            axis = 1
                        ) \
                        .between(236, 244) # 240 hours +/- 4 hours
                    )
                )
            ].tmp_row.tolist()

        toxbatch_args.update({
            "dataframe": toxbatch,
            "tablename": 'tbl_toxbatch',
            "badrows": badrows,
            "badcolumn": "species, matrix, actualtestduration",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "For records with species Eohaustorius estuarius, the ActualTestDuration must be between 92 and 100 hours (about 4 days) or between 236 and 244 hours (about 10 days)."
        })
        errs = [*errs, checkData(**toxbatch_args)]
        
        

        # ERROR - Mytilus galloprovincialis/Reference or Sediment Water Interface (Regardless of matrix) 48hours or 2 days 
        print("## ERROR - Mytilus galloprovincialis 48hours or 2 days (regardless of matrix##")
        # For MG regardless of matrix the ActualTestDuration should be around 48 hours or 2 days. 
        badrows = toxbatch[
                (
                    ( toxbatch["species"] == "Mytilus galloprovincialis") 
                )
                & 
                ( 
                    # ActualTestDurationUnits can only can be "Hours" or "Days" - it is tied to a lookup list lu_toxtestunits
                    ~toxbatch.apply(
                        lambda row: row["actualtestduration"] if row["actualtestdurationunits"] == 'Hours' else row["actualtestduration"] * 24,
                        axis = 1
                    ) \
                    .between(44, 52) # 48 hours +/- 4 hours
                )
            ] \
            .tmp_row \
            .tolist()

        toxbatch_args.update({
            "dataframe": toxbatch,
            "tablename": 'tbl_toxbatch',
            "badrows": badrows,
            "badcolumn": "species, matrix, actualtestduration",
            "error_type": "Logic Error",
            "is_core_error": False,
            "error_message": "For records with Mytilus galloprovincialis and matrix of either Reference Toxicant or Sediment Water Interface, the ActualTestDuration must be 44 to 52 hours (about 2 days)."
        })
        errs = [*errs, checkData(**toxbatch_args)]

        # Check - Strongylocentrotus purpuratus/Reference or Whole Sediment 72hours or 3 days 
        # print("## Check - Strongylocentrotus purpuratus/Reference or Whole Sediment 72hours or 3 days ##")
        # Check: For SP with matrix RT or WS the ActualTestDuration should be around 72 hours or 3 days. 

        # This was only for bight 23 intercalibration
        # badrows = toxbatch[
        #         (toxbatch["species"] == "Strongylocentrotus purpuratus")
        #         & 
        #         ( 
        #             # ActualTestDurationUnits can only can be "Hours" or "Days" - it is tied to a lookup list lu_toxtestunits
        #             ~toxbatch.apply(
        #                 lambda row: row["actualtestduration"] if row["actualtestdurationunits"] == 'Hours' else row["actualtestduration"] * 24,
        #                 axis = 1
        #             ) \
        #             .between(68, 76) # 72 hours +/- 4 hours
        #         )
        #     ] \
        #     .tmp_row.tolist()

        # toxbatch_args.update({
        #     "dataframe": toxbatch,
        #     "tablename": 'tbl_toxbatch',
        #     "badrows": badrows,
        #     "badcolumn": "species, actualtestduration",
        #     "error_type": "Logic Error",
        #     "is_core_error": False,
        #     "error_message": "For records with Strongylocentrotus purpuratus, the ActualTestDuration should be 68 to 76 hours (about 3 days)."
        # })
        # warnings = [*warnings, checkData(**toxbatch_args)]
        ## END BATCH CHECKS ##

        ## RESULT CHECKS ##
        print("Starting Toxicity Result Checks")

        # Sample Assignment checks
        # Commented out 7/20/2023
        # badrows = sample_assignment_check(eng = eng, df = toxresults,  parameter_column = 'species')
        # toxresults_args.update({
        #     "badrows": badrows,
        #     "badcolumn": "StationID,Species,Lab",
        #     "error_type": "Logic Error",
        #     "error_message": f"Your lab was not assigned to this species for this station (<a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=vw_sample_assignment&datatype=toxicity target=_blank>see sample assignments</a>) Be sure the sampletypecode says 'QA' rather than 'Grab'"
        # })
        # warnings = [*warnings, checkData(**toxresults_args)]

        # For tox, they are supposed to submit the stations they were not assigned to - but the sampletypecode should not say "Grab", but rather "QA"
        # For this reason, this above code is commented out, and replaced by the code below

        # Now issue an error if they put Grab for a station they were not assigned to (check results tab only)
        # Get their assigned stations
        assigned_tox_stations = pd.read_sql(
            "SELECT DISTINCT stationid, parameter AS species, assigned_agency AS lab, 'yes' AS assigned FROM sample_assignment_table WHERE LOWER(datatype) = 'toxicity'; ", 
            eng
        )
        chkdf = toxresults.merge(assigned_tox_stations, how = 'left', on = ['stationid','species','lab'])
        chkdf.assigned = chkdf.assigned.fillna('no')
        badrows = chkdf[(chkdf.stationid.astype(str) != '0000') & (chkdf.assigned == 'no') & (chkdf.sampletypecode != 'QA')].tmp_row.tolist()
        toxresults_args.update({
            "badrows": badrows,
            "badcolumn": "StationID,SampleTypeCode",
            "error_type": "Logic Error",
            "error_message": f"Your lab was not assigned to this species for this station (<a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=vw_sample_assignment&datatype=toxicity target=_blank>see sample assignments</a>) The sampletypecode should say 'QA' rather than 'Grab'"
        })
        errs = [*errs, checkData(**toxresults_args)]
        

        # 1. CHECK IF SAMPLES WERE TESTED WITHIN 28 DAY HOLDING TIME
        print("## CHECK IF SAMPLES WERE TESTED WITHIN 28 DAY HOLDING TIME ##")
        # merge result and batch on toxbatch but include teststartdate
        df28 = pd.merge(toxresults, toxbatch[['toxbatch', 'teststartdate']].drop_duplicates(), how = 'left', on = 'toxbatch')
        # change the following field types to pandas datetime so they can be calculated (we arent changing submitted data)
        df28['teststartdate'] = pd.to_datetime(df28['teststartdate'])
        df28['samplecollectdate'] = pd.to_datetime(df28['samplecollectdate'])
        # put day differences into own column
        df28['checkdate'] = df28['teststartdate'] - df28['samplecollectdate']
        # locate any records with a greater than 28 period
        print(df28.loc[df28['checkdate'].dt.days > 28])

        # Jan 2, 2024
        # Darrin said some samples (stations) had split samples (Not sure what that means) that would be tested after the 28 day holding time
        # He said he didnt care about those meeting the 28 day requirement
        # Stations which are excepted, and have QA sampletypecodes will not have this check applied to them
        
        # Basically we aren't going to flag them since we are expecting them to test within the normal holding time
        #    and they werent assigned the sample anyways in the first place
        
        holding_time_exception_stations = ['B23-12060','B23-12065']
        holding_time_error_rows = df28.loc[ (df28['checkdate'].dt.days > 28) & ((df28.sampletypecode != 'QA') | (~df28.stationid.isin(holding_time_exception_stations)) ) ].tmp_row.tolist()
        holding_time_warning_rows = df28.loc[ (df28['checkdate'].dt.days > 28) & ((df28.sampletypecode == 'QA') & (df28.stationid.isin(holding_time_exception_stations)) ) ].tmp_row.tolist()

        # Warn them if its a QA sampletypecode for one of the excepted stations
        toxresults_args.update({
            "badrows": holding_time_warning_rows,
            "badcolumn": "sampletypecode",
            "error_type": "Undefined Warning",
            "error_message": "Samples should be tested within a 28 day holding time."
        })
        warnings = [*warnings, checkData(**toxresults_args)] 
        
        # Prevent data submission if they do not meet the exception requirements
        toxresults_args.update({
            "badrows": holding_time_error_rows,
            "badcolumn": "sampletypecode",
            "error_type": "Undefined Error",
            "error_message": "Samples must be tested within a 28 day holding time."
        })
        errs = [*errs, checkData(**toxresults_args)] 
        


        toxresults_args.update({
            "badrows": df28.loc[df28['checkdate'].dt.days < 0].tmp_row.tolist(),
            "badcolumn": "sampletypecode",
            "error_type": "Logic Error",
            "error_message": "You have entered a SampleCollectDate that comes after the corresponding TestStartDate specified in the batch tab"
        })
        errs.append(checkData(**toxresults_args))
        
        toxresults_args.update({
            "badrows": df28[(df28['sampletypecode'] == 'RFNH3') & (df28['checkdate'].dt.days != 0)].tmp_row.tolist(),
            "badcolumn": "sampletypecode",
            "error_type": "Logic Error",
            "error_message": "For Reference Toxicant batches, the samplecollectdate (In results tab) must be the same as the teststartdate (In the batch tab)"
        })
        errs.append(checkData(**toxresults_args))



        # 2. REFERENCE TOXICANT IN THE MATRIX FIELD MUST HAVE DATA IN CONCENTRATION FIELD. CAN'T BE -88.
        print("## REFERENCE TOXICANT IN THE MATRIX FIELD MUST HAVE DATA IN CONCENTRATION FIELD. CANT BE -88 ##")
        print(toxresults.loc[toxresults['matrix'].isin(['Reference Toxicant','RT']) & (toxresults['concentration'] == -88)])
        badrows = toxresults.loc[toxresults['matrix'].isin(['Reference Toxicant','RT']) & (toxresults['concentration'] == -88)].tmp_row.tolist()
        toxresults_args.update({
            "badrows": badrows,
            "badcolumn": "concentration",
            "error_type": "Undefined Error",
            "error_message": "A Reference Toxicant record in the Matrix field can not have a -88 in the Concentration field."
        })
        errs = [*errs, checkData(**toxresults_args)] 
        
        # Check #3 
        # A LAB IS ASSIGNED BOTH STATIONS AND TEST SPECIES. CHECK TO SEE IF THE SUBMISSION MATCHES BOTH.
        # This check was commented out in the old checker
        
        # 4. QACODE CHECK - A SINGLE QACODE IS REQUIRED BUT MULTIPLE QACODES ARE POSSIBLE (MANY TO MANY). author - Jordan Golemo
        print("QACODE CHECK - A SINGLE QACODE IS REQUIRED BUT MULTIPLE QACODES ARE POSSIBLE (MANY TO MANY)")
        tmpargs = multivalue_lookup_check(toxresults, 'qacode', 'lu_toxtestacceptability', 'testacceptability', dbconnection = eng, displayfieldname = 'QACode')
        toxresults_args.update(tmpargs)
        errs = [*errs, checkData(**toxresults_args)]

        # 5. ENDPOINT PERCENT NORMAL-ALIVE IS SPECIES SPECIFIC TO TO MG OR SG.
        print("## ENDPOINT PERCENT NORMAL-ALIVE IS SPECIES SPECIFIC TO MG OR SP##")
        badrows = toxresults[((toxresults["species"] != "Mytilus galloprovincialis") & (toxresults["species"] != "Strongylocentrotus purpuratus")) & (toxresults["endpoint"] == "Percent normal-alive")].tmp_row.tolist()
        toxresults_args.update({
            "badrows": badrows,
            "badcolumn": "species, endpoint",
            "error_type": "Undefined Error",
            "error_message": "Endpoint Percent Normal-alive is species specific to either Mytilus galloprovincialis or Strongylocentrotus purpuratus."
        })
        errs = [*errs, checkData(**toxresults_args)] 
        
        
        # 6. WHEN SAMPLETYPE CODE IS 'CNEG' OR 'CNSL' RESULT SHOULD BE DIFFERENT FOR EACH TOXBATCH GROUP
        print(" WHEN SAMPLETYPE CODE IS 'CNEG' OR 'CNSL' RESULT SHOULD BE DIFFERENT FOR EACH TOXBATCH GROUP ")
        #filter sampletypecode and matrix
        filtered_result = toxresults[toxresults['sampletypecode'].isin(['CNEG']) & ~toxresults['matrix'].isin(['Reference Toxicant'])]
        filtered_batch =  toxbatch[~toxbatch['matrix'].isin(['Reference Toxicant'])]
        #Merge the two dataframes on toxbatch and matrix to get specific columns from toxbatch
        merged_df = pd.merge(filtered_result, filtered_batch[['toxbatch', 'teststartdate', 'matrix']], on=['matrix','toxbatch'], how='inner')
        print(merged_df)
        #group by 'toxbatch', 'stationid','teststartdate','lab','species' then make a list of all the 
        # results in each group and another list for the origianl index
        grouped = merged_df.groupby(['toxbatch', 'stationid','teststartdate','lab','species']).agg({
            'result': pd.Series.tolist,
            'tmp_row':pd.Series.tolist}).\
                reset_index(). \
                    sort_values('result')
        print(grouped)
        #change the result column to string so we can group the same ones togther later
        grouped['result'] = grouped['result'].astype(str)
        #find duplicated result lists
        grouped = grouped[grouped['result'].duplicated(keep=False)]
        #change result column to str so that we can group the indices together based on results   
        grouped['result'] = grouped['result'].astype(str)
        #perform a groupby on result and then make add the indices list together to become one list, and make a list of the toxbatches
        duplicate_result_df = grouped[grouped['result'].duplicated(keep=False)]
        print(duplicate_result_df)
        badresults = duplicate_result_df.groupby(['teststartdate','species','result']).agg({
            'tmp_row':pd.Series.sum,
            'toxbatch':pd.Series.tolist}).\
                reset_index()
        print(f"badresults: {badresults}")
        #looping through badresults to get the rows that have two or more toxbatches in the list indicating that they have duplicated results
        for i,row in badresults.iterrows():
            if len(row['toxbatch'])>1:
                print(row['toxbatch'])
                toxresults_args.update({
                "badrows": row.tmp_row,
                "badcolumn": "result,toxbatch",
                "error_type": "Undefined Warning",
                "error_message": f"The batches {', '.join(row.toxbatch)} have  the same result values."
                })
                errs = [*errs, checkData(**toxresults_args)]

        ## END RESULT CHECKS ##

        ## START WQ CHECKS ##
        print("Starting Toxicity WQ Checks")
        print("Starting Toxicity WQ Checks")
        # 1. CHECK THAT WATER QUALITY PARAMETERS ARE WITHIN ACCEPTABLE RANGES. - WARNING ONLY NOT ERROR MESSSAGE
        # merge wq and batch on toxbatch to get species from batch
        dfwq = pd.merge(toxwq[['toxbatch','parameter','result','matrix','tmp_row']], toxbatch[['toxbatch', 'species']], how = 'left', on = 'toxbatch')
        
        # For EE amd MG, and the parameter is Temperature, result should be between 13 and 17
        print(dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','Mytilus galloprovincialis','EE','MG'])) & (dfwq['parameter'] == 'Temperature') & ((dfwq['result'] < 13) | (dfwq['result'] > 17))])
        badrows = dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','Mytilus galloprovincialis','EE','MG'])) & (dfwq['parameter'] == 'Temperature') & ((dfwq['result'] < 13) | (dfwq['result'] > 17))].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for Temperature not in acceptable range: must be between 13-17."
        })
        warnings = [*warnings, checkData(**toxwq_args)]
        
        # For EE amd MG, and the parameter is Salinity, result should be between 30 and 34
        print(dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','Mytilus galloprovincialis','EE','MG'])) & (dfwq['parameter'] == 'Salinity') & ((dfwq['result'] < 30) | (dfwq['result'] > 34))])
        badrows = dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','Mytilus galloprovincialis','EE','MG'])) & (dfwq['parameter'] == 'Salinity') & ((dfwq['result'] < 30) | (dfwq['result'] > 34))].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for Salinity not in acceptable range: must be between 30-34."
        })
        warnings = [*warnings, checkData(**toxwq_args)]
        
        # For EE, and the parameter Dissolved Oxygen, result should be less than 7.5
        badrows = dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','EE'])) & (dfwq['parameter'] == 'Dissolved Oxygen') & (dfwq['result'] <= 7.5)].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for Dissolved Oxygen not in acceptable range: must be greater than 7.5."
        })
        warnings = [*warnings, checkData(**toxwq_args)]

        # For EE, and the parameter pH, result should be between 7.7 and 8.3
        badrows = dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','EE'])) & (dfwq['parameter'] == 'pH') & ((dfwq['result'] < 7.7) | (dfwq['result'] > 8.3))].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for pH not in acceptable range: must be between 7.7-8.3."
        })
        warnings = [*warnings, checkData(**toxwq_args)]

        
        badrows = dfwq.loc[(dfwq['species'].isin(['Eohaustorius estuarius','EE'])) & (dfwq['parameter'] == 'Total Ammonia') & (dfwq['result'] > 20)&(dfwq['matrix']!='Reference Toxicant')].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for Total Ammonia not in acceptable range: must be less than 20."
        })
        warnings = [*warnings, checkData(**toxwq_args)]

        
        print(dfwq.loc[(dfwq['species'].isin(['Mytilus galloprovincialis','MG'])) & (dfwq['parameter'] == 'Dissolved Oxygen') & (dfwq['result'] < 4.0)])
        badrows = dfwq.loc[(dfwq['species'].isin(['Mytilus galloprovincialis','MG'])) & (dfwq['parameter'] == 'Dissolved Oxygen') & (dfwq['result'] < 4.0)].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for Dissolved Oxygen not in acceptable range: must be greater than 4.0."
        })
        warnings = [*warnings, checkData(**toxwq_args)]
        
        
        print(dfwq.loc[(dfwq['species'].isin(['Mytilus galloprovincialis','MG'])) & (dfwq['parameter'] == 'pH') & ((dfwq['result'] < 7.6) | (dfwq['result'] > 8.3))])
        badrows = dfwq.loc[(dfwq['species'].isin(['Mytilus galloprovincialis','MG'])) & (dfwq['parameter'] == 'pH') & ((dfwq['result'] < 7.6) | (dfwq['result'] > 8.3))].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "result",
            "error_type": "Undefined Warning",
            "error_message": "Water quality parameter for paramter pH not in acceptable range: must be between 7.6-8.3."
        })
        warnings = [*warnings, checkData(**toxwq_args)]


        # Check - Timepoint should be an integer between -1 and 10
        badrows = toxwq[~toxwq.timepoint.between(-1, 10)].tmp_row.tolist()
        toxwq_args.update({
            "badrows": badrows,
            "badcolumn": "timepoint",
            "error_type": "Value Error",
            "error_message": "Timepoint must be an integer from -1 to 10 (i.e. the units should be in terms of days, not hours) (use -1 to denote a measurement taken upon receiving the sample)"
        })
        errs = [*errs, checkData(**toxwq_args)]

        
        #UPDATE: Jordan - Make sure all Species/SampleTypeCode groups have all parameters present in data.
        #        Jordan - Check that all water quality parameters are present at required time points (beginning and end of test and on an every-other-day basis in between)
        print("## Check that all water quality parameters are present at required time points ##")
        dfwq = pd.merge(
            toxwq[['timepoint','parameter','sampletypecode','toxbatch','tmp_row']], toxbatch[['species','toxbatch', 'actualtestduration','actualtestdurationunits']],
            how = 'left',
            on='toxbatch'
        )
        
        # For the sake of checking required timepoints
        dfwq['testduration'] = dfwq.apply(
            lambda row:
            round(row['actualtestduration'] / 24)
            if row['actualtestdurationunits'] == 'Hours'
            else row['actualtestduration']
            , axis = 1
        )

        dfwq = dfwq[['timepoint','parameter','sampletypecode','toxbatch','species','testduration','tmp_row']]
        print("dfwq")
        print(dfwq)
        print("dfwq.columns")
        print(dfwq.columns)
        print("dfwq['toxbatch'].ndim")
        print(dfwq['toxbatch'].ndim)
        print("dfwq['species'].ndim")
        print(dfwq['species'].ndim)
        print("dfwq['sampletypecode'].ndim")
        print(dfwq['sampletypecode'].ndim)
        print("dfwq.index.names")
        print(dfwq.index.names)




        # Creates series that consists of sets of submitted parameters
        # pgs = dfwq.groupby(['toxbatch','species','sampletypecode'])['parameter'].apply(set).reset_index()
        print("group by 'dfwq' to check for missing params")
        pgs = dfwq.groupby(['toxbatch','species','sampletypecode']).agg({
                'parameter' : set,
                'tmp_row'   : list
            }) \
            .reset_index()

        # Determines whether all appropriate parameters have been submitted
        print("# Determines whether all appropriate parameters have been submitted")
        pgs['missing'] = pgs.parameter.apply(lambda x: set(['Dissolved Oxygen', 'Salinity', 'Temperature', 'pH', 'Total Ammonia', 'Unionized Ammonia']) - x)
        
        # Provide Error for any missing parameters
        print("# Provide Error for any missing parameters")
        kk = pgs[pgs.missing != set()]

        if not kk.empty:
            tmpargslist = kk.apply(
                lambda row:
                {
                    "badrows"       : row.tmp_row,
                    "badcolumn"     : "Timepoint",
                    "error_type"    : "Missing Required Data",
                    "error_message" : f"For the batch {row.toxbatch} (species {row.species} and sampletype {row.sampletypecode}), the following required parameters are missing: {sorted(list(row.missing))}"
                },
                axis = 1
            )
            

            for args in tmpargslist:
                toxwq_args.update(args)
                errs = [*errs, checkData(**toxwq_args)]

        # pg = dfwq.groupby(['toxbatch','parameter','species','sampletypecode'])['timepoint'].apply(set).reset_index()
        # CORRECTION: For Total and Unionized Ammonia Parameters with species EE, only required to make measurements for 0,10. With species MG 0,2. 
        # For EE & Unionized or Total Ammonia (CNEG/GRAB/QA):

        # trying to preserve tmp_row of the problematic records
        print("group dfwq to check for missing timepoints")
        pg = dfwq.groupby(['toxbatch','parameter','species','sampletypecode','testduration']).agg({
                'tmp_row'   : list,
                'timepoint' : set
            }) \
            .reset_index()
        
        # Get timepoints for start and end of test, as well as every even day from start to end of test
        print("# Get timepoints for start and end of test, as well as every even day from start to end of test")
        pg['start_and_end_timepoints'] = pg.testduration.apply(lambda t: set( [0, int(t)] ) )
        pg['all_timepoints'] = pg.testduration.apply(lambda t: set( [t_ for t_ in range(int(t) + 1) if ((t_ % 2) == 0) ] ) )
                
        
        # NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE #
        #  3/29/2023 - Darrin said these timepoint values should be in Hours rather than days                      #
        #  7/18/2023 - Darrin said these it doesnt matter as long as it is consistent - we are going with days     #
        # NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE NOTE #

        # The criteria is as follows - regardless of species or sampletype, they need timepoints at the start and end of the test for Ammonia, 
        #   and timepoints every other day (example 0,2,4...) for all other parameters

        print("Get the missing timepoints")
        pg['missing'] = pg.apply(
            lambda row:
            (row['start_and_end_timepoints'] if 'ammonia' in str(row['parameter']).lower() else row['all_timepoints']) - row['timepoint']
            ,
            axis = 1
        )

        # Provide Error for any missing timepoints
        print("Provide Error for any missing timepoints")
        k = pg[pg.missing != set()]
        print(k)

        if not k.empty:
            tmpargslist = k.apply(
                lambda row:
                {
                    "badrows"       : row.tmp_row,
                    "badcolumn"     : "Timepoint",
                    "error_type"    : "Missing Required Data",
                    "error_message" : f"For the batch {row.toxbatch} (species {row.species} and sampletype {row.sampletypecode}), the parameter {row.parameter} is missing a measurement at timepoint(s): {','.join([str(x) for x in sorted(list(row.missing))])}"
                },
                axis = 1
            ).tolist()

            for args in tmpargslist:
                toxwq_args.update(args)
                errs = [*errs, checkData(**toxwq_args)]


        # for i in k.index:
        #     if (k.sampletypecode[i] != 'CNSL') and (not pd.isnull(k.missing[i])):
        #         badrows = dfwq[(dfwq.toxbatch == k.toxbatch[i])&(dfwq.species == k.species[i])].tmp_row.tolist()
        #         toxwq_args.update({
        #             "badrows": badrows,
        #             "badcolumn": "timepoint",
        #             "error_type": "Undefined Error",
        #             "error_message": f'Associated water quality group {k.parameter[i]}/{k.species[i]}/{k.sampletypecode[i]} is missing time points {list(k.missing[i])}.'
        #         })
        #         errs = [*errs, checkData(**toxbatch_args)]
    
    ## END WQ CHECKS ##
    ## END CUSTOM CHECKS ##
    
    
    # clean up the errors list
    errs = [e for e in errs if len(e) > 0]
    

    #################################################
    ### ---------- SUMMARY TABLE START ---------- ###
    #################################################
    
    
    if len(errs) == 0:
        # summary must not be a groupby otherwise below functions wont work
        print("Creating Toxicity Summary Results Table")

        df_match = copy.deepcopy(toxresults)
        print(df_match.head())
        print(df_match.columns)
        print("Building Calculated Columns")
        def getCalculatedValues(grp):                                  
            grp['mean'] = grp[grp.result != -88]['result'].mean()
            #grp['n'] = grp['fieldreplicate'].sum() - bug n values was returning incorrect sum due to merge with grab table above
            grp['n'] = len(grp[grp.result != -88].tmp_row.tolist())
            grp['stddev'] = grp[grp.result != -88]['result'].std()
            grp['variance'] = grp['stddev'].apply(lambda x: x ** 2 )
            if grp['mean'].unique().item() != float(0):
                grp['coefficientvariance'] = ((grp['stddev']/grp['mean']) * 100)
            else:
                grp['coefficientvariance'] = 0
            return grp
        
        grouping_columns = ['stationid','toxbatch','sampletypecode','samplecollectdate','treatment','concentration']

        print("1")
        print(list(df_match.groupby(grouping_columns)))
        toxsummary = df_match.groupby(grouping_columns).apply(getCalculatedValues)
        
        print("summary")
        print(toxsummary)

        # get all control records
        print("# get all control records")
        cneg = toxsummary[grouping_columns + ['mean']].where(toxsummary['sampletypecode'] == 'CNEG')
        # get all non control records
        print("# get all non control records")
        nocneg = toxsummary[grouping_columns + ['mean']].where(toxsummary['sampletypecode'] != 'CNEG')

        # get all reference toxicant records just save them for now
        # drop all reference toxicants from the summary dataframe - not a part of summary results
        
        toxsummary = toxsummary.loc[~toxsummary['matrix'].isin(['Reference Toxicant'])]

        cneg = cneg.dropna()
        
        nocneg = nocneg.dropna()
        

        cneg['unique'] = np.nan
        nocneg['unique'] = np.nan

        control_mean = cneg.groupby(grouping_columns + ['mean'])['unique'].nunique().reset_index()
        
        control_mean_dict = control_mean.set_index('toxbatch')['mean'].to_dict()

        # copy control_mean dataframe column mean to controlvalue
        control_mean['controlvalue'] = control_mean['mean']
        toxsummary = toxsummary.merge(control_mean[['toxbatch','controlvalue']], how = 'left', on = ['toxbatch'])

        def getPctControl(row):
            ## toxbatch control should always be 100
            if(row['sampletypecode'] == 'CNEG'):
                row['pctcontrol'] = 100
            elif row['toxbatch'] in control_mean_dict:
                # if the toxbatch is in the lookup dictionary then
                # divide the result mean from the control mean and times by 100
                row['pctcontrol'] = ((row['mean']/control_mean_dict[row['toxbatch']]) * 100)
            else:
                row['pctcontrol'] = np.nan
            return row
        toxsummary = toxsummary.apply(getPctControl, axis=1)

        print("toxsummary")
        print(toxsummary.head())

        ## author - Tyler Vu
        def getPValue(summary):
            for index, values in summary['toxbatch'].iteritems():
                station_code = summary.loc[index, 'stationid']
                cneg_result = summary[['result']].where((summary['sampletypecode'] == 'CNEG') & (summary['toxbatch'] == values))
                
                # for "result both", we need to exclude the sampletypecode of 'QA' and CNSL (per Darrin)
                # Robert Butler - Jan 30 2023
                result_both = summary[['result']].where(
                    (summary['toxbatch'] == values) 
                    & (summary['stationid'] == station_code) 
                    & (summary['sampletypecode'] != 'QA')
                    & (summary['sampletypecode'] != 'CNSL')
                )
                #plus it was causing a critical and i dont know why
                cneg_result = cneg_result.dropna()
                result_both = result_both.dropna()
                t, p = stats.ttest_ind(cneg_result, result_both, equal_var = False)
                print("pvalue t: %s, p: %s" % (t,p))
                
                summary.loc[index, 'tstat'] = 100 if str(t).lower() == 'inf' else t
                single_tail = p/2
                #summary.loc[index, 'pvalue'] = p/2 #we divide by 2 to make it a 1 tailed
                summary.loc[index, 'pvalue'] = single_tail #we divide by 2 to make it a 1 tailed
                if (t < 0):
                    summary.loc[index, 'sigeffect'] = 'NSC'
                else:
                    if (single_tail <= .05):
                        summary.loc[index, 'sigeffect'] = 'SC'
                    else:
                        summary.loc[index, 'sigeffect'] = 'NSC'
        getPValue(toxsummary)
        print("done w getPValue")

        ## author - Tyler Vu 
        def getSQO(grp):
            #if(grp['species'] == 'EE'): - coded values
            if(grp['species'] == 'Eohaustorius estuarius'):
                if(grp['mean'] < 90):
                    if (grp['pctcontrol'] < 82):
                        if (grp['pctcontrol'] < 59):
                            grp['sqocategory'] = 'High Toxicity'
                        else:
                            if (grp['sigeffect'] == 'NSC'):
                                grp['sqocategory'] = 'Low Toxicity'
                            else:
                                grp['sqocategory'] = 'Moderate Toxicity'
                    else:
                        if (grp['sigeffect'] == 'NSC'):
                            grp['sqocategory'] = 'Nontoxic'
                        else:
                            grp['sqocategory'] = 'Low Toxicity'
                else:
                    grp['sqocategory'] = 'Nontoxic'
            #elif (grp['species'] == 'MG'): - coded values
            elif (grp['species'] == 'Mytilus galloprovincialis'):
                if (grp['mean'] < 80):
                    if (grp['pctcontrol'] < 77):
                        if (grp['pctcontrol'] < 42):
                                grp['sqocategory'] = 'High Toxicity'
                        else:
                            if (grp['sigeffect'] == 'NSC'):
                                grp['sqocategory'] = 'Low Toxicity'
                            else:
                                grp['sqocategory'] = 'Moderate Toxicity'
                    else:
                        if (grp['sigeffect'] == 'NSC'):
                            grp['sqocategory'] = 'Nontoxic'
                        else:
                            grp['sqocategory'] = 'Low Toxicity'
                else:
                    grp['sqocategory'] = 'Nontoxic'
            return grp

        print("calling getSQO")
        toxsummary = toxsummary.apply(getSQO, axis=1)

        ### SUMMARY TABLE END ###
    
    

        # ORGANIZE SUMMARY OUTPUT
        # results no database fields
        
        # with database fields
        # rename a few columns to match with existing b13 column names
        toxsummary.rename(columns={"resultunits": "units"}, inplace=True)
        # set p and tstat values if they are empty to -88
        toxsummary['tstat'].fillna(-88,inplace=True)
        toxsummary = toxsummary.replace(np.inf, 1000)
        toxsummary['pvalue'].fillna(-88,inplace=True)

        # get summary dataframe with error columns before it is replaced - bug fix number 37 below for duplicate summary rows
        print("# get summary dataframe with error columns before it is replaced - bug fix number 37 below for duplicate summary rows")
        toxsummary = toxsummary.drop_duplicates(subset = ['stationid','toxbatch','fieldreplicate','pvalue'],keep='first')
        toxsummary.reset_index(inplace = True, drop = True)
        toxsummary.drop('tmp_row', axis = 1, inplace = True)
        

        ## SUMMARY TABLE CHECKS ##
        # adding tmp_row column to back into toxsummary df (after it was removed right before summary table checks run)
        toxsummary = toxsummary.assign(tmp_row = toxsummary.index)
        
        print("Starting Toxicity Summary Result Checks")
        toxsummary_args = {
            "dataframe": toxsummary,
            "tablename" : current_app.config.get("TOXSUMMARY_TABLENAME"),
            "badrows": [],
            "badcolumn": "",
            "error_type": "",
            "is_core_error": False,
            "error_message": ""
        }
        # 1 - WARNING TO CHECK FOR DATA ENTRY ERRORS IF THE STANDARD DEVIATION FOR A SAMPLE EXCEEDS 50 
        print("## WARNING TO CHECK FOR DATA ENTRY ERRORS IF THE STANDARD DEVIATION FOR A SAMPLE EXCEEDS 50 ##")
        print(toxsummary.loc[(toxsummary["stddev"] > 50)])
        badrows = toxsummary.loc[(toxsummary["stddev"] > 50)].tmp_row.tolist()
        toxsummary_args.update({
            "badrows": badrows,
            "badcolumn": "stddev",
            "error_type": "Undefined Warning",
            "error_message": 'Warning standard deviation exceeds 50.'
        })
        warnings = [*warnings, checkData(**toxsummary_args)]

        print("toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 90)]")
        print(toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 90)])
        toxsummary_args.update({
            "badrows": toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 90)].tmp_row.tolist(),
            "badcolumn": "mean",
            "error_type": "Undefined Error",
            "error_message": 'Does not meet control acceptability criterion; mean control value < 90'
        })
        errs = [*errs, checkData(**toxsummary_args)]
        
        print("toxsummary[(toxsummary['species'].isin(['Mytilus galloprovincialis','MG'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 80)]")
        print(toxsummary[(toxsummary['species'].isin(['Mytilus galloprovincialis','MG'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 80)])
        toxsummary_args.update({
            "badrows": toxsummary[(toxsummary['species'].isin(['Mytilus galloprovincialis','MG'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['mean'] < 80)].tmp_row.tolist(),
            "badcolumn": "mean",
            "error_type": "Undefined Error",
            "error_message": 'Does not meet control acceptability criterion; mean control value < 80'
        })
        errs = [*errs, checkData(**toxsummary_args)]
        print("toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['coefficientvariance'] > 11.9)]")
        print(toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['coefficientvariance'] > 11.9)])
        toxsummary_args.update({
            "badrows": toxsummary[(toxsummary['species'].isin(['Eohaustorius estuarius','EE'])) & (toxsummary['sampletypecode'] == 'CNEG') & (toxsummary['coefficientvariance'] > 11.9)].tmp_row.tolist(),
            "badcolumn": "coefficientvariance",
            "error_type": "Undefined Error",
            "error_message": 'Does not meet control acceptability criterion; coefficient of variance should be under 11.9'
        })
        errs = [*errs, checkData(**toxsummary_args)]


        # ## END SUMMARY TABLE CHECKS ##

        # Drop the columns that are in the dataframe, but not in the database table
        #toxsummary.drop(list(set(toxsummary.columns) - set(analysis_table_cols)), axis = 1, inplace = True)


        fielddata = pd.read_sql(
            """
                SELECT DISTINCT
                    tbl_stationoccupation.stationid,
                    tbl_stationoccupation.occupationlatitude as latitude,
                    tbl_stationoccupation.occupationlongitude as longitude,tbl_stationoccupation.occupationdepth as stationwaterdepth,
                    tbl_stationoccupation.occupationdepthunits as stationwaterdepthunits,
                    field_assignment_table.areaweight,
                    field_assignment_table.stratum
                FROM field_assignment_table 
                    INNER JOIN tbl_stationoccupation 
                    ON field_assignment_table.stationid = tbl_stationoccupation.stationid
                WHERE 
                    tbl_stationoccupation.collectiontype = 'Grab' 
                    AND tbl_stationoccupation.stationfail = 'None or No Failure'
            """,
            eng
        )

        toxsummary = toxsummary.merge(fielddata, how = 'left', on = 'stationid')

        analysis_table_cols = pd.read_sql(f"""SELECT column_name FROM information_schema.columns WHERE table_name = '{current_app.config.get("TOXSUMMARY_TABLENAME")}';""", eng).column_name.tolist()
        toxsummary.drop(
            list(set(toxsummary.columns) - set(analysis_table_cols)),
            axis = 'columns',
            inplace = True
        )

        print("toxsummary")
        print(toxsummary)

        # write tox summary to the submission excel file
        with pd.ExcelWriter(session.get('excel_path'), engine = 'openpyxl', mode = 'a') as writer:
            toxsummary.to_excel(writer, index = False, sheet_name=current_app.config.get("TOXSUMMARY_TABLENAME"))


        # The session table to tab map must be updated so that the excel markup routine can find the newly created sheet
        # it also must be updated so that the javascript can correctly build the error report for the user to see in the browser
        session['table_to_tab_map'][current_app.config.get("TOXSUMMARY_TABLENAME")] = current_app.config.get("TOXSUMMARY_TABLENAME")


        print("summary end")
        ## END SUMMARY TABLE CHECKS ##

        # For now leave it in since the toxsummary doesnt go through match routine
        # we might need this later for debugging and troubleshooting
        # print("toxsummary")
        # print(toxsummary)
        # print(toxsummary.columns)
        # for c in toxsummary.columns:
        #     print(f"column: {c}")
        #     print(toxsummary[c])

        ################################################
        # WARNING: CHECK AND SEE IF DATAFRAME NAME AND TBLNAME IS CORRECT
        ################################################

        


    return {'errors': errs, 'warnings': warnings}

