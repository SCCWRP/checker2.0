# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData, checkLogic, sample_assignment_check, mismatch
from .chem_functions_custom import *
import pandas as pd
import re

def chemistry(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # DB Connection
    eng = g.eng

    # define errors and warnings list
    errs = []
    warnings = []

    batch = all_dfs['tbl_chembatch']
    results = all_dfs['tbl_chemresults']

    batch['tmp_row'] = batch.index
    results['tmp_row'] = results.index

    # Tack on analyteclass
    results = results.merge(
        pd.read_sql("""SELECT analyte AS analytename, analyteclass FROM lu_analytes""", eng),
        on = 'analytename',
        how = 'inner'
    )
    
    # Calculation of percentrecovery was moved to the QA plan checks section

    # sampleid should just be everything before the last occurrence of a hyphen character in the labsampleid
    # if no hyphen, the sampleid is just the labsampleid
    # checker_labsampleid is created in main.py for storing purposes in case it may be needed on how data is grouped for later use
    results['sampleid'] = results.labsampleid.apply(lambda x: str(x).rpartition('-')[0 if '-' in str(x) else -1]  )

    batch_args = {
        "dataframe": batch,
        "tablename": 'tbl_chembatch',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    results_args = {
        "dataframe": results,
        "tablename": 'tbl_chemresults',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    
    # Check to see if GrainSize was submitted along with Sediment Results
    grain_analytes = pd.read_sql("SELECT analyte FROM lu_analytes WHERE analyteclass = 'GrainSize';", eng).analyte.tolist()
    GRAIN_BOOL_SERIES = results.analytename.isin(grain_analytes)
    

    # ----- LOGIC CHECKS ----- # 
    print('# ----- LOGIC CHECKS ----- # ')

    # chem submission must have a corresponding grabevent record (Where sediment chemistry/grainsize was collected)
    print('# chem submission must have a corresponding grabevent record')

    matchcols = ['stationid','sampledate']
    grabevent = pd.read_sql(
        f"SELECT DISTINCT stationid, sampledate FROM tbl_grabevent WHERE UPPER({ 'grainsize' if all(GRAIN_BOOL_SERIES) else 'sedimentchemistry' }) = 'YES';", eng
    )
    
    print("""results[results.stationid.str.lower() != '0000']""")
    print(results[results.stationid.str.lower() != '0000'])
    print("grabevent")
    print(grabevent)
    
    results_args.update({
        "badrows": mismatch(results[results.stationid.str.lower() != '0000'], grabevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in chemistry results must have a corresponding record in tbl_grabevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**results_args)]



    # Batch and Results must have matching records on Lab, PreparationBatchID and SampleID

    # check records that are in batch but not in results
    # checkLogic function is not being used since it marks incorrect rows on marked excel file return
    # Check for records in batch but not results
    badrows = batch[~batch[['lab','preparationbatchid']].isin(results[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    batch_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Batch must have a matching record in Chemistry Results. Records are matched on Lab and PreparationBatchID."
    })
    errs.append(checkData(**batch_args))

    # Check for records in results but not batch
    badrows = results[~results[['lab','preparationbatchid']].isin(batch[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Results must have a matching record in Chemistry Batch. Records are matched on Lab and PreparationBatchID."
    })
    errs.append(checkData(**results_args))


    # if there is a mixture of analyteclasses (GrainSize and non-GrainSize) the data should be flagged
    if not ((all(GRAIN_BOOL_SERIES)) or (all(~GRAIN_BOOL_SERIES))):
        n_grain = sum(GRAIN_BOOL_SERIES)
        n_nongrain = sum(~GRAIN_BOOL_SERIES)

        # If there are less grainsize records, flag them as being the bad rows. Otherwise flag the non grainsize rows
        results_args.update({
            "badrows": results[(GRAIN_BOOL_SERIES) if (n_grain < n_nongrain) else (~GRAIN_BOOL_SERIES)].tmp_row.tolist(),
            "badcolumn": "AnalyteName",
            "error_type": "Logic Error",
            "error_message": "You are attempting to submit grainsize analytes along with other sediment chemistry analytes. Sediment Chemistry Results must be submitted separately from Grainsize data"
        })
        errs.append(checkData(**results_args))
        
        # If they have mixed data, stop them here for the sake of time
        return {'errors': errs, 'warnings': warnings}

    # Sample Assignment check - make sure they were assigned the analyteclasses that they are submitting
    badrows = sample_assignment_check(eng = eng, df = results, parameter_column = 'analyteclass', excepted_params = ['Moisture'])
    
    results_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,Lab,AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"Your lab was not assigned to submit data for this analyteclass from this station (<a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=vw_sample_assignment target=_blank>see sample assignments</a>)"
    })
    warnings.append(checkData(**results_args))

    # Check - A sediment chemistry submission cannot have records with a matrix of "tissue"
    results_args.update({
        "badrows": results[results.matrix == 'tissue'].tmp_row.tolist(),
        "badcolumn": "Matrix",
        "error_type": "Logic Error",
        "error_message": f"This is a sediment chemistry submission but this record has a matrix value of 'tissue'"
    })
    errs.append(checkData(**results_args))

    # PFAS completeness checks - checking for blanks
    pfas_field_blank_stations = pd.read_sql(f"""SELECT DISTINCT stationid FROM vw_sample_assignment WHERE "parameter" = 'PFAS Field Blank' ORDER BY 1""", g.eng).stationid.tolist()
    pfasresults = results[results.analyteclass == 'PFAS']
    if not pfasresults.empty:
        # Check - if a lab is submitting PFAS then they need to also submit the field blanks
        pfas_field_blank_results = pfasresults[pfasresults.stationid.isin(pfas_field_blank_stations)]
        if not pfas_field_blank_results.empty:
            # Among stations for which they are giving us PFAS, and which are asssigned field blanks - they need to be giving us that data
            checkdf = pfas_field_blank_results.groupby(['stationid']).agg(
                {
                    'sampletype': (lambda col: 'Field blank' in col.unique()),
                    'tmp_row' : list
                }
            ).reset_index().rename(columns = {'sampletype': 'has_field_blank'})
            
            bad = checkdf[~checkdf.has_field_blank]
            if not bad.empty:
                for _, row in bad.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row,
                        "badcolumn": "StationID,SampleType",
                        "error_type": "Missing Data",
                        "error_message": f"""The station {row.stationid} was assigned as PFAS field blank but it appears to be missing from your submission"""
                    })
                    errs.append(checkData(**results_args))


        # Check - if a lab is submitting PFAS then they need to also submit the equipment blanks
        # One PFAS equipment blank per lab

        # Get labs that have given us equipment blanks
        print('# Get labs that have given us equipment blanks')
        equipblanks = pd.read_sql("SELECT DISTINCT lab FROM tbl_chemresults WHERE sampletype = 'Equipment blank'; ", g.eng)
        
        
        # Filter down to the records where the lab is NOT in the list of labs that have already given equipment blanks
        # if equipblanks is an empty dataframe, disregard - doing equipblanks.lab will give an error in that case
        checkdf = pfasresults[~pfasresults.lab.isin(equipblanks.lab.tolist())]
        
        if not checkdf.empty:
            checkdf = checkdf.groupby(['lab','analytename']).agg(
                    {
                        'sampletype': (lambda col: 'Equipment blank' in col.unique()),
                        'tmp_row' : list
                    }
                ).reset_index().rename(columns = {'sampletype': 'has_equipment_blank'})
            
            bad = checkdf[~checkdf.has_equipment_blank]
            if not bad.empty:
                for _, row in bad.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row,
                        "badcolumn": "Lab,SampleType",
                        "error_type": "Missing Data",
                        "error_message": f"""You are submitting PFAS data but there is no Equipment blank provided for the analyte {row.analytename} """
                    })
                    errs.append(checkData(**results_args))
                    


    # ----- END LOGIC CHECKS ----- # 
    print('# ----- END LOGIC CHECKS ----- # ')

        
    # ----- CUSTOM CHECKS - GRAINSIZE RESULTS ----- #
    if all(GRAIN_BOOL_SERIES):
        print('# ----- CUSTOM CHECKS - GRAINSIZE RESULTS ----- #')
        
        # Check - Result column should be a positive number (except -88) for all SampleTypes (Error)
        print("""# Check - Result column should be a positive number (except -88) for all SampleTypes (Error)""")
        badrows = results[(results.result != -88) & (results.result <= 0)].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": "The Result column should be a positive number (unless it is -88)"
        })
        errs.append(checkData(**results_args))
        
        
        errs.append(checkData(**results_args))
        # Check - Units must be %
        results_args.update({
            "badrows": results[results.units != '%'].tmp_row.tolist(),
            "badcolumn": "Units",
            "error_type": "Value Error",
            "error_message": "For GrainSize data, units must be %"
        })
        errs.append(checkData(**results_args))
        
        # Check - for each grouping of stationid, fieldduplicate, labreplicate, the sum of the results should be between 99.8 and 100.2
        tmp = results.groupby(['stationid','fieldduplicate','labreplicate']).apply(lambda df: df.result.fillna(0).replace(-88, 0).sum())
        if tmp.empty:
            return {'errors': errs, 'warnings': warnings}

        tmp = tmp.reset_index(name='resultsum')
        tmp = tmp[(tmp.resultsum < 99.8) | (tmp.resultsum > 100.2)]
        
        if tmp.empty:
            return {'errors': errs, 'warnings': warnings}

        checkdf = results.merge(tmp, on = ['stationid','fieldduplicate','labreplicate'], how = 'inner')
        checkdf = checkdf \
            .groupby(['stationid','fieldduplicate','labreplicate','resultsum']) \
            .apply(lambda df: df.tmp_row.tolist()) \
            .reset_index(name='badrows')

        tmp_argslist = checkdf.apply(
            lambda row: 
            {
                "badrows": row.badrows,
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"For this grouping of StationID: {row.stationid}, FieldDuplicate: {row.fieldduplicate}, and LabReplicate: {row.labreplicate}, the sum of the results was {row.resultsum}, which is outside of a range we would consider as normal (99.8 to 100.2)"
            },
            axis = 1
        ).values

        for argset in tmp_argslist:
            results_args.update(argset)
            errs.append(checkData(**results_args))
        
        print('# ----- END CUSTOM CHECKS - GRAINSIZE RESULTS ----- #')
        return {'errors': errs, 'warnings': warnings}
    # ----- END CUSTOM CHECKS - GRAINSIZE RESULTS ----- #
    

    # ----- CUSTOM CHECKS - SEDIMENT RESULTS ----- #
    print('# ----- CUSTOM CHECKS - SEDIMENT RESULTS ----- #')

    # Check for All required analytes per station (All or nothing)
    current_matrix = 'sediment' #sediment or tissue - affects query for required analytes
    # Check for all required analytes per station - if a station has a certain analyteclass
    req_anlts = pd.read_sql(f"SELECT analyte AS analytename, analyteclass FROM lu_analytes WHERE b23{current_matrix} = 'yes' AND analyteclass != 'Pyrethroid'; ", eng) \
        .groupby('analyteclass')['analytename'] \
        .apply(set) \
        .to_dict()

    # Reference materials will not always have all the analytes
    # labs may enter -88 for those analytes which dont have reference material values
    chkdf = results.groupby(['stationid','sampletype','analyteclass'])['analytename'].apply(set).reset_index()
    chkdf['missing_analytes'] = chkdf.apply(
        lambda row: ', '.join(list((req_anlts.get(row.analyteclass) if req_anlts.get(row.analyteclass) is not None else set()) - row.analytename)), axis = 1 
    )

    chkdf = chkdf[chkdf.missing_analytes != set()]
    if not chkdf.empty:
        chkdf = results.merge(chkdf[chkdf.missing_analytes != ''], how = 'inner', on = ['stationid','sampletype','analyteclass'])
        chkdf = chkdf.groupby(['stationid','sampletype','analyteclass','missing_analytes']).agg({'tmp_row': list}).reset_index()

        # add error or warning as a key value pair to the args dictionary
        # it will be "popped" off later to determine whether we will issue a warning or error
        # this check is a warnings for the Reference Material sampletypes but an error for everything else
        errs_args = chkdf.apply(
            lambda row:
            {
                "error_or_warning": "warning" if ('Reference' in str(row.sampletype)) else "error",
                "badrows": row.tmp_row,
                "badcolumn" : "stationid",
                "error_type": "Missing Data",
                "error_message" : f"For the station {row.stationid}, and sampletype {row.sampletype} you attempted to submit {row.analyteclass} but are missing some required analytes ({row.missing_analytes})"
            },
            axis = 1
        ).tolist()

        for argset in errs_args:
            error_or_warning = argset.pop("error_or_warning")
            results_args.update(argset)
            if error_or_warning == "error":
                errs.append(checkData(**results_args))
            else:
                warnings.append(checkData(**results_args))

    # End of checking all required analytes per station, if they attempted submission of an analyteclass

    # Separate check for the Pyrethroid analyteclass
    pyre = pd.read_sql(f"SELECT analyte AS analytename FROM lu_analytes WHERE b23{current_matrix} = 'yes' AND analyteclass = 'Pyrethroid' AND NOT (analyte ~ 'Permethrin'); ", eng).analytename.tolist()

    # filter to Pyrethroid analyteclass, groupby stationid and get stations where the set of Pyrethroids is not a subset of the Pyrethroids being submitted for that station
    # We are excluding the Permethrin analytes for this check
    pyrethresults = results[ results.analyteclass == 'Pyrethroid' ]
    if not pyrethresults.empty:
        # in order to be considered a complete set of pyrethroids:
        #  the set of analytenames must contain all from the query AND at least one of the analytes must have "Permethrin" in it
        badstations = pyrethresults.groupby('stationid')['analytename'].apply( lambda x: not ((set(pyre).issubset(set(x))) and (any([ 'Permethrin' in str(anlt) for anlt in x ])) ) )
        badstations = badstations[badstations == True].index.tolist()

        badrows = results[(results.stationid.isin(badstations)) & (results.analyteclass == 'Pyrethroid')].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "StationID,AnalyteName",
            "error_type": "Incomplete Data",
            "error_message": f"""You are submitting Pyrethroid data for the stations {','.join(badstations)} but they appear to be missing some of the Pyrethroid analytes (see the <a href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=lu_analytes>lookup list</a>)"""
        })
        errs.append(checkData(**results_args))

    # Special check for Pyrethroids courtesy of chatGPT:
    # Within a batch (analysisbatchid) if you have "Permethrin, cis" you must also have "Permethrin, trans"
    # if you have "Permethrin, trans" you must also have "Permethrin, cis"
    # If you have "Permethrin (cis + trans)" you must not have "Permethrin, cis" nor "Permethrin, trans"
    def check_permethrin(group):
        perm_cis = group[group['analytename'] == 'Permethrin, cis']['tmp_row'].tolist()
        perm_trans = group[group['analytename'] == 'Permethrin, trans']['tmp_row'].tolist()
        perm_cis_trans = group[group['analytename'] == 'Permethrin (cis + trans)']['tmp_row'].tolist()

        # If 'Permethrin, cis' exists but 'Permethrin, trans' doesn't or vice versa
        rule1_violation = perm_cis + perm_trans if (perm_cis and not perm_trans) or (perm_trans and not perm_cis) else []
        
        # If 'Permethrin (cis + trans)' exists and either 'Permethrin, cis' or 'Permethrin, trans' exists
        rule2_violation = perm_cis_trans if perm_cis_trans and (perm_cis or perm_trans) else []

        return rule1_violation + rule2_violation

    if not pyrethresults.empty:

        # Apply function to each batch
        badrows = pyrethresults.groupby('analysisbatchid').apply(check_permethrin)

        # Flatten the list of lists
        badrows = [row for sublist in badrows for row in sublist]
        
        results_args.update({
            "badrows": badrows,
            "badcolumn": "AnalysisBatchID,AnalyteName",
            "error_type": "Logic Error",
            "error_message": f"""Within a batch (AnalysisBatchID) you may have the results for 'Permethrin, cis' and 'Permethrin, trans' recorded separately OR the sum represented by the analytename 'Permethrin (cis + trans)' """
        })
        errs.append(checkData(**results_args))


    # Check - in a sediment submission, they should be reporting % by weight of Moisture for all samples
    if not results.empty:
        # I cannot think of a single case where results here would be empty, but i always put that
        
        checkdf = results[(results.matrix == 'sediment') & (results.stationid != '0000')]
        checkdf = checkdf[['stationid','sampledate','analytename','lab','tmp_row']]
        
        # Looking to see if the lab has submitted moisture before for that stationcode/sampledate
        # Let them off the hook if they submitted in a previous submission
        dbmoisture = pd.read_sql(
            "SELECT stationid, sampledate, analytename, lab, -99 AS tmp_row FROM tbl_chemresults WHERE analytename = 'Moisture' ", 
            eng
        )
        checkdf = pd.concat(
            [checkdf, dbmoisture[dbmoisture.lab.isin(checkdf.lab.unique())]], 
            ignore_index = True
        )
        
        if not checkdf.empty:
            checkdf = checkdf.groupby(['stationid','sampledate']).agg({
                    # True if Moisture is in there, False otherwise
                    'analytename' : (lambda grp: 'Moisture' in grp.unique()), 
                    'tmp_row': list
                }) \
                .reset_index() \
                .rename(columns = {'analytename': 'has_moisture'}) # rename to has_moisture since really that is what the column is representing after the groupby operation
            
            bad = checkdf[~checkdf.has_moisture]
            if not bad.empty:
                for _, row in bad.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row,
                        "badcolumn": "StationID,SampleDate,AnalyteName",
                        "error_type": "Missing Data",
                        "error_message": f"""For the station {row.stationid} and sampledate {row.sampledate} it appears the sediment moisture was not reported"""
                    })
                    errs.append(checkData(**results_args))




    # End of the checks for No partial submissions of analyteclasses



    # ------------------------- Begin chemistry base checks ----------------------------- #


    # Check for duplicates on stationid, sampledate, analysisbatchid, sampletype, matrix, analytename, fieldduplicate, labreplicate, SAMPLEID
    # Cant be done in Core since there is no sampleid column that we are having them submit, but rather it is a field we create internally based off the labsampleid column
    dupcols = ['analysisbatchid', 'sampletype', 'matrix', 'analytename', 'fieldduplicate', 'labreplicate', 'sampleid']
    
    # Technically doing sort_values is unnecessary and irrelevant, 
    #   but if you were to test the code and examine, you would see that it would put the duplicated records next to each other
    #   duplicated() needs keep=False argument to flag all duplicated rows instead of marking last occurrence 
    results_args.update({
        "badrows": results.sort_values(dupcols)[results.duplicated(dupcols, keep=False)].tmp_row.tolist(),
        "badcolumn" : 'AnalysisBatchID,SampleType,Matrix,AnalyteName,FieldDuplicate,LabReplicate,LabSampleID',
        "error_type": "Value Error",
        "error_message" : "These appear to be duplicated records that need to be distinguished with the labreplicate field (labrep 1, and labrep 2)"
    })
    errs.append(checkData(**results_args))


    # Check - if the sampletype is "Result" or "Field blank" then the stationid cannot be '0000' 
    results_args.update({
        "badrows": results[results.sampletype.isin(['Result','Field blank']) & results.stationid.isin(['0000']) ].tmp_row.tolist(),
        "badcolumn" : 'SampleType,StationID',
        "error_type": "Value Error",
        "error_message" : "If the sampletype is 'Result' or 'Field blank' then the stationid must not be '0000'"
    })
    errs.append(checkData(**results_args))
    
    # Check - if stationid is not 0000, then the sampletype cannot be "Result" or "Field blank"
    # Not applying this check to Matrix spike sampletype - that one will get its own check
    results_args.update({
        "badrows": results[ (results.sampletype != 'Matrix spike') & ((~results.stationid.isin(['0000'])) & ~results.sampletype.isin(['Result','Field blank'])) ].tmp_row.tolist(),
        "badcolumn" : 'SampleType,StationID',
        "error_type": "Value Error",
        "error_message" : "if stationid != '0000' then sampletype should be 'Result' or 'Field blank'"
    })
    errs.append(checkData(**results_args))

    # Check - if the sampletype is "Equipment blank" then the stationid must be '0000' 
    results_args.update({
        "badrows": results[results.sampletype.isin(['Equipment blank']) & (~results.stationid.isin(['0000'])) ].tmp_row.tolist(),
        "badcolumn" : 'SampleType,StationID',
        "error_type": "Value Error",
        "error_message" : "if sampletype is Equipment blank, then the stationid must be 0000 - Equipment blanks are not considered to be associated with any particular station" 
    })
    errs.append(checkData(**results_args))
    
    # Check - if the stationid is not 0000 and the sampletype is Matrix spike, then it is an error
    # They should use the QA stationID with a correct QA code if they used the actual station sediment as the matrix for spiking
    results_args.update({
        "badrows": results[ (~results.stationid.isin(['0000'])) & (results.sampletype == 'Matrix spike') ].tmp_row.tolist(),
        "badcolumn" : 'SampleType,StationID',
        "error_type": "Value Error",
        "error_message" : f"All QA sampletypes must have a stationid of 0000 (Including Matrix spikes). If this is a Matrix spike done with the actual sediment sample, you should use the appropriate <a href={current_app.script_root}/scraper?action=help&layer=lu_chemqacodes>QA Code</a> ('Matrix spike done with the actual sediment sample as the matrix for spiking')"
    })
    errs.append(checkData(**results_args))
    

    # Check - if the qacode is 'Matrix spike done with the actual sediment sample as the matrix for spiking' then a comment containing the station it came from is required
    print("# Check - if the qacode is 'Matrix spike done with the actual sediment sample as the matrix for spiking' then a comment containing the station it came from is required")
    results_args.update({
        "badrows": results[ 
            (results.qacode == 'Matrix spike done with the actual sediment sample as the matrix for spiking') 
            & 
            (~results.comments.fillna("").str.contains(r'B23-\d{5}', regex = True) )
            &
            # SONGS Unit 2 was a special station that was sampled, which does not fit the conventional Bight 23 station naming pattern
            (~results.comments.fillna("").str.contains(r'B23-SONGS Unit 2', regex = True) )
            
        ].tmp_row.tolist(),
        "badcolumn" : 'Comments',
        "error_type": "Value Error",
        "error_message" : "If the qacode is 'Matrix spike done with the actual sediment sample as the matrix for spiking' then we ask that a comment is provided telling which station it came from"
    })
    errs.append(checkData(**results_args))
    print("# END OF Check - if the qacode is 'Matrix spike done with the actual sediment sample as the matrix for spiking' then a comment containing the station it came from is required")


    # Check - for PFAS analytes, warning if there is a value that says "labwater"
    results_args.update({
        "badrows": results[
            (results.analyteclass.isin(["PFAS"])) & ( results.matrix == 'labwater' )
        ].tmp_row.tolist(),
        "badcolumn" : "matrix",
        "error_type": "Value error",
        "error_message" : "For PFAS analytes, and for blank sampletypes, you should use 'PFAS-free water' as the matrix rather than 'labwater'. If you actually did not use PFAS-free water, then please leave a comment in the comments column"
    })
    warnings.append(checkData(**results_args))


    # Check - for PFAS analytes, error if there is a value that says "labwater" but no corresponding comment
    results_args.update({
        "badrows": results[
            ((results.analytename.isin(["PFOA","PFOS"])) & ( results.matrix == 'labwater' )) & (results.comments.fillna('').astype(str).replace('\s*','',regex=True) == '')
        ].tmp_row.tolist(),
        "badcolumn" : "matrix",
        "error_type": "Value error",
        "error_message" : "For PFAS analytes, and for blank sampletypes, you should use 'PFAS-free water' as the matrix rather than 'labwater'. If you did not use PFAS-free water, a comment is required"
    })
    errs.append(checkData(**results_args))



    # Check - If the sampletype is "Lab blank", "Field blank", "Equipment blank", or "Blank spiked" then the matrix must be labwater or PFAS-free water
    results_args.update({
        "badrows": results[
            (results.sampletype.isin(["Lab blank","Blank spiked"])) & (~results.matrix.isin(["labwater","PFAS-free water"]))
        ].tmp_row.tolist(),
        "badcolumn" : "matrix",
        "error_type": "Value error",
        "error_message" : "If the sampletype is Lab blank or Blank spiked, the matrix must be 'labwater' or 'PFAS-free water'"
    })
    errs.append(checkData(**results_args))


    # Check - If the sampletype is "Field blank" or "Equipment blank" then the analyte should be a PFAS analyte
    results_args.update({
        "badrows": results[(results.sampletype.isin(["Field blank", "Equipment blank"])) & (~results.analyteclass.isin(["PFAS"]))].tmp_row.tolist(),
        "badcolumn" : "sampletype,analytename",
        "error_type": "Value error",
        "error_message" : "If the sampletype is 'Field blank' or 'Equipment blank' then the analyte should be a PFAS analyte"
    })
    errs.append(checkData(**results_args))


    # Check - If the sampletype is "Field blank" or "Equipment blank" then the matrix must be PFAS-free water
    results_args.update({
        "badrows": results[(results.sampletype.isin(["Field blank","Equipment blank"])) & (~results.matrix.isin(["PFAS-free water"]))].tmp_row.tolist(),
        "badcolumn" : "matrix",
        "error_type": "Value error",
        "error_message" : "If the sampletype is 'Field blank' or 'Equipment blank' then the matrix must be PFAS-free water"
    })
    errs.append(checkData(**results_args))
    
    
    # Check - If the matrix is labwater, then the sampletype must be Lab blank or Blank spiked
    results_args.update({
        "badrows": results[ (results.matrix == 'labwater') & (~results.sampletype.isin(["Lab blank","Blank spiked"])) ].tmp_row.tolist(),
        "badcolumn" : "matrix, sampletype",
        "error_type": "Value error",
        "error_message" : "If the matrix is labwater, then the sampletype must be Lab blank or Blank spiked"
    })
    errs.append(checkData(**results_args))
    
    
    # Check - If the matrix is PFAS-free water, then the sampletype must be Field blank or Equipment blank
    results_args.update({
        "badrows": results[ (results.matrix == 'PFAS-free water') & (~results.sampletype.isin(["Field blank","Equipment blank"])) ].tmp_row.tolist(),
        "badcolumn" : "matrix, sampletype",
        "error_type": "Value error",
        "error_message" : "If the matrix is PFAS-free water, then the sampletype must be Field blank or Equipment blank"
    })
    errs.append(checkData(**results_args))


    # Check - TrueValue must be -88 for everything except CRM's and spikes (Matrix Spikes and blank spikes)
    # This is to be a Warning rather than an error
    # checked lu_sampletypes as of 11/18/2022 and these two cases will cover Reference Materials, blank spikes and matrix spikes
    # case = False makes the string match not case sensitive
    spike_mask = results.sampletype.str.contains('spike', case = False) | results.sampletype.str.contains('Reference', case = False)

    # badrows are deemed to be ones that are not in the "spike" or Reference category, but the TrueValue column is NOT a -88 (Warning)
    print('# badrows are deemed to be ones that are not in the "spike" or Reference category, but the TrueValue column is NOT a -88 (Warning)')
    badrows = results[(~spike_mask) & (results.truevalue != -88)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is not a Matrix spike, Blank spiked or a CRM Reference Material, so the TrueValue should be -88"
    })
    warnings.append(checkData(**results_args))
    
    # badrows here could be considered as ones that ARE spikes (matrix spike and blank spiked), but the TrueValue is missing (Warning)
    print('# badrows here could be considered as ones that ARE spikes (matrix spike and blank spiked), but the TrueValue is missing (Warning)')
    spike_only_mask = results.sampletype.str.contains('spike', case = False)
    badrows = results[(spike_only_mask) & ((results.truevalue <= 0) | results.truevalue.isnull())].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is a Matrix spike or Blank spiked, so the TrueValue should not be -88 (or any negative number)"
    })
    warnings.append(checkData(**results_args))


    # Check - Result column should be a positive number (except -88) for all SampleTypes (Error)
    print("""# Check - Result column should be a positive number (except -88) for all SampleTypes (Error)""")
    badrows = results[(results.result != -88) & (results.result <= 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "The Result column for all SampleTypes should be a positive number (unless it is -88)"
    })
    errs.append(checkData(**results_args))

    # Check - The MDL should never be greater than the RL (Error)
    print('# Check - The MDL should never be greater than the RL (Error)')
    results_args.update({
        "badrows": results[results.mdl > results.rl].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be greater than the RL in most cases"
    })
    warnings.append(checkData(**results_args))
    
    # Check - The MDL should not be equal to the RL (Warning)
    print('# Check - The MDL should not be equal to the RL (Warning)')
    results_args.update({
        "badrows": results[results.mdl == results.rl].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be equal the RL in most cases"
    })
    warnings.append(checkData(**results_args))
    
    # Check - The MDL should never be a negative number (Error)
    print('# Check - The MDL should never be a negative number (Error)')
    results_args.update({
        "badrows": results[results.mdl < 0].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be negative"
    })
    errs.append(checkData(**results_args))


    # Check - if result > RL then the qualifier cannot say "below reporting limit or "below method detection limit"
    print('# Check - if result > RL then the qualifier cannot say "below reporting limit or "below method detection limit"')
    results_args.update({
        "badrows": results[(results.result > results.rl) & (results.qualifier.isin(['below reporting limit','below method detection limit']))].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": """if result > RL then the qualifier cannot say 'below reporting limit' or 'below method detection limit'"""
    })
    errs.append(checkData(**results_args))


    # Check - if the qualifier is "less than or equal to" or "below method detection limit" Then the result must be -88 (Error)
    print('# Check - if the qualifier is "less than or equal to" or "below method detection limit" Then the result must be -88 (Error)')
    results_args.update({
        "badrows": results[results.qualifier.isin(["less than or equal to", "below method detection limit"]) & (results.result.astype(float) != -88)].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'less than or equal to' or 'below method detection limit' then the Result should be -88"
    })
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "estimated" or "below reporting limit" then the result must be between the mdl and rl (inclusive) EXCEPT Lab blank sampletypes (Error)
    print('# Check - if the qualifier is "estimated" or "below reporting limit" then the result must be between the mdl and rl (inclusive) EXCEPT Lab blank sampletypes (Error)')
    results_args.update({
        "badrows": results[
                ((results.qualifier.isin(["estimated", "below reporting limit"])) & (~results.sampletype.isin(['Lab blank', 'Field blank', 'Equipment blank']) ))
                & (
                    (results.result < results.mdl) | (results.result > results.rl)
                )
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'estimated' or 'below reporting limit' then the Result should be between the MDL and RL (Inclusive). (This does not apply to Lab blanks, Field blanks or Equipment blanks.)"
    })
    errs.append(checkData(**results_args))
    
    # Check - if the qualifier is less than or equal to, below mdl, below reporting limit, or estimated, but the result > rl, then the wrong qualifier was used
    print('# Check - if the qualifier is less than or equal to, below mdl, below reporting limit, or estimated, but the result > rl, then the wrong qualifier was used')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["less than or equal to", "below reporting limit", "below method detection limit", "estimated"])) 
                & (results.result > results.rl)
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "if qualifier is 'less than or equal to', 'below method detection limit', 'below reporting limit' or 'estimated', but the Result > RL, then the incorrect qualifier was used"
    })
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "none" then the result must be greater than the RL (Error) Except lab blanks
    print('# Check - if the qualifier is "none" or "equal to" then the result must be greater than the RL (Error) Except lab/field/equipment blanks')
    results_args.update({
        "badrows": results[
            (
                (results.qualifier.isin(['none', 'equal to'])) & (~results.sampletype.isin(['Lab blank', 'Field blank', 'Equipment blank']) )
            ) & 
            (results.result <= results.rl)
        ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "if the qualifier is 'none' or 'equal to' then the result must be greater than the RL (Except Lab, Field, Equipment blanks)"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.comments.fillna('').str.replace('\s*','', regex = True) == '')].tmp_row.tolist(),
        "badcolumn": "Comments",
        "error_type": "Value Error",
        "error_message": "Comment is required if the qualifier says 'analyst error' 'contaminated' or 'interference'"
    })
    errs.append(checkData(**results_args))

    
    # Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)
    print('# Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)')
    results_args.update({
        "badrows": results[results.qualifier == "analyst error"].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "We would like to be contacted concerning this record of data. Please contact b23-im@sccwrp.org"
    })
    warnings.append(checkData(**results_args))


    # ----------------------------------------------------------------------------------------------------------------------------------#
    # Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)
    print('# Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)')
    # Analyte classes: Inorganics, PAH, PCB, Chlorinated Hydrocarbons, Pyrethroid, PBDE, FIPRONIL, Lipids, TN, TOC
    # Required sampletypes by analyteclass:
    # Inorganics: Lab blank, Reference Material, Matrix Spike, Blank Spike
    # PAH: Lab blank, Reference Material, Matrix Spike
    # PCB, Chlorinated Hydrocarbons, PBDE: Lab blank, Reference Material, Matrix Spike
    # Pyrethroid, FIPRONIL: Lab blank, Matrix Spike
    # TN: Lab blank
    # TOC: Lab blank, Reference Material
    error_args = []
    
    # Robert - Removed 1944 Sed from required sampletypes
    # @Zaib there is that check below that uses the "check_required_crm" function - does it work?
    required_sampletypes = {
        "Inorganics": ['Lab blank', 'Blank spiked', 'Result','Reference - ERA 540 Sed'],
        "PAH": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "PCB": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "Chlorinated Hydrocarbons": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "PBDE": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "PFAS": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "Pyrethroid": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "Neonicotinoids": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "TIREWEAR": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result'],
        "TN" : ['Lab blank', 'Result'],
        "TOC" : ['Lab blank', 'Result']
    }

    # anltclass = analyteclass
    # smpltyps = sampletypes
    # Just temp variables for this code block
    for anltclass, smpltyps in required_sampletypes.items():
        print("Check required sampletypes")
        error_args = [*error_args, *chk_required_sampletypes(results, smpltyps, anltclass)]
        
        print("Check for sample duplicates or matrix spike duplicates")
        if anltclass != 'Inorganics':
            print('Organics')
            # For Inorganics, they can have either or, so the way we deal with inorganics must be different
            # error_args = [*error_args, *check_dups(results, anltclass, 'Result')]
            error_args = [*error_args, *check_dups(results, anltclass, ('Matrix spike' if anltclass not in ('TOC','TN') else 'Result') )] 
        else:
            print('Inorganics')
            # Under the assumption of how we worded the error message. 
            # This will be to grab the batches that failed both the duplicate results and duplicate matrix spike check
            # Each batch has tp have at least one of those

            batch_regex = re.compile('The\s+AnalysisBatch\s+([^\s]*)')
            resargs = check_dups(results, anltclass, 'Result')
            spikeargs = check_dups(results, anltclass, 'Matrix spike')
            
            # If one of the lists is empty, then it means every single batch had a duplicate, 
            #  meaning the data is clean
            if ( (len(resargs) > 0) and (len(spikeargs) > 0) ):
                # make them into dataframes
                res = pd.DataFrame(resargs)
                res['batch'] = res.apply(
                    lambda row: 
                    re.search(batch_regex, row.error_message).groups()[0]
                    if re.search(batch_regex, row.error_message)
                    else '',
                    axis = 1
                )

                spike = pd.DataFrame(spikeargs)
                spike['batch'] = spike.apply(
                    lambda row: 
                    re.search(batch_regex, row.error_message).groups()[0]
                    if re.search(batch_regex, row.error_message)
                    else '',
                    axis = 1
                )

                argsdf = res.merge(spike[['batch']], on = 'batch', how = 'inner')

                if not argsdf.empty:
                    argsdf.error_message = argsdf.apply(
                        lambda row: f"""The AnalysisBatch {row.batch} needs either a duplicate sample result or a duplicate matrix spike""",
                        axis = 1
                    )
                    error_args = [*error_args, *argsdf.to_dict('records')]

    
    # NOTE needs to be updated
    requires_crm = ["Inorganics", "PAH", "PCB", "Chlorinated Hydrocarbons", "PBDE", "TOC"]
    error_args = [*error_args, *check_required_crm(results, requires_crm)]
    
    for argset in error_args:
        results_args.update(argset)
        errs.append(checkData(**results_args))

    
    
    # ---------------------------------------------------------------------------------------------------------------------------------- #

    # Check - For sampletype Lab blank, if Result is less than MDL, it must be -88
    print('# Check - For sampletype Lab blank, if Result is less than MDL, it must be -88')
    # mb_mask = Lab blank mask (lab blank is also called method blank)
    print('# mb_mask = Lab blank mask')
    mb_mask = (results.sampletype.isin(['Lab blank', 'Field blank', 'Equipment blank']))
    results_args.update({
        "badrows": results[mb_mask & ((results.result < results.mdl) & (results.result != -88))].tmp_row.tolist(),
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "For Lab blank/Equipment blank/Field blank sampletypes, if Result is less than MDL, it must be -88"
    })
    errs.append(checkData(**results_args))

    # Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.
    print('# Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.')
    results_args.update({
        "badrows": results[(mb_mask & (results.result == -88)) & (~results.qualifier.isin(['below method detection limit','none'])) ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "If SampleType=Lab blank, Equipment blank, or Field blank, and Result=-88, then qualifier must be 'below method detection limit' or 'none'"
    })
    errs.append(checkData(**results_args))

    # Check - True Value should not be Zero
    print('# Check - True Value should not be Zero')
    results_args.update({
        "badrows": results[results.truevalue == 0].tmp_row.tolist(),
        "badcolumn": "truevalue",
        "error_type": "Value Error",
        "error_message": "The TrueValue should never be zero. If the TrueValue is unknown, then please fill in the cell with -88"
    })
    errs.append(checkData(**results_args))


    # ---------------------------------------------------------------------------------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------#
    print('# ---------------------------------------------------------------------------------------------------------------------------------#')
    # Check - Holding times for AnalyteClasses: 
    print('# Check - Holding times for AnalyteClasses: ')
    #  Inorganics, PAH, PCB, Chlorinated Hydrocarbons, PBDE, Pyrethroid, TOC/TN is 1 year (see notes)
    print('#  Inorganics, PAH, PCB, Chlorinated Hydrocarbons, PBDE, Pyrethroid, TOC/TN is 1 year (see notes)')

    holding_time_mask = (results.analysisdate - results.sampledate >= timedelta(days=365))
    holding_time_classes = ['Inorganics', 'PAH', 'PCB', 'Chlorinated Hydrocarbons', 'PBDE', 'Pyrethroid', 'TOC', 'TN']
    results_args.update({
        "badrows": results[
                results.analyteclass.isin(holding_time_classes) 
                & holding_time_mask
            ].tmp_row.tolist(),
        "badcolumn": "SampleDate, AnalysisDate",
        "error_type": "Sample Past Holding Time",
        "error_message": f"Here, the analysisdate is more than a year after the sampledate, which is invalid for analyteclasses {','.join(holding_time_classes)}"
    })
    warnings.append(checkData(**results_args))

    # NOTE The Holding time for Mercury is 6 months, so a separate check will be written here specifically for Mercury
    print('# NOTE The Holding time for Mercury is 6 months, so a separate check will be written here specifically for Mercury')
    # months is not allowed in a timedelta so here we put 183 days instead
    print('# months is not allowed in a timedelta so here we put 183 days instead')
    Hg_holding_time_mask = ((results.analysisdate - results.sampledate >= timedelta(days=183)) & (results.analytename == 'Mercury'))
    results_args.update({
        "badrows": results[Hg_holding_time_mask].tmp_row.tolist(),
        "badcolumn": "SampleDate, AnalysisDate",
        "error_type": "Sample Past Holding Time",
        "error_message": f"Here, the analysisdate is more than 6 months after the sampledate, which is past the holding time for Mercury"
    })
    warnings.append(checkData(**results_args))
    # ---------------------------------------------------------------------------------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------#
    print('# ---------------------------------------------------------------------------------------------------------------------------------#')
    


    # # ------------------------------------------------------------------------------------------------------------#
    # print('# ------------------------------------------------------------------------------------------------------------#')
    # # Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both: - disabled since values do not exist in lookup - zaib 2may2025
    # print('# Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:')
    # # 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"
    # print('# 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"')
    # # 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"
    # print('# 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"')
    # # 3. "Permethrin, cis" and "Permethrin (cis/trans)"
    # print('# 3. "Permethrin, cis" and "Permethrin (cis/trans)"')
    # # 4. "Permethrin, trans" and "Permethrin (cis/trans)"
    # print('# 4. "Permethrin, trans" and "Permethrin (cis/trans)"')

    # results_args.update(pyrethroid_analyte_logic_check(results, ["Deltamethrin/Tralomethrin", "Deltamethrin"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Esfenvalerate/Fenvalerate", "Esfenvalerate"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, cis", "Permethrin (cis/trans)"]))
    # errs.append(checkData(**results_args))
    # results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, trans", "Permethrin (cis/trans)"]))
    # errs.append(checkData(**results_args))
    
    # # END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......
    # print('# END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......')
    # ------------------------------------------------------------------------------------------------------------#
    print('# ------------------------------------------------------------------------------------------------------------#')

    # Check - If sampletype is a Reference material, the matrix cannot be "labwater" - it must be sediment
    print('# Check - If sampletype is a Reference material, the matrix cannot be "labwater" - it must be sediment')
    results_args.update({
        "badrows": results[results.sampletype.str.contains('Reference', case = False) & (results.matrix != 'sediment')].tmp_row.tolist(),
        "badcolumn": "SampleType, Matrix",
        "error_type": "Value Error",
        "error_message": f"If sampletype is a Reference material, the matrix cannot be 'labwater' - Rather, it must be sediment"
    })
    errs.append(checkData(**results_args))
    

    # -----------------------------------------------------------------------------------------------------------------------------------#

    # Check - if the analytename is 'Moisture','TOC','TN then the unit must be % by weight
    results_args.update({
        "badrows": results[ results.analytename.isin(['Moisture','TOC','TN']) & (results.units != r'% by weight') ].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": r"if the analytename is 'Moisture','TOC' or 'TN' then the unit must be % by weight"
    })
    errs.append(checkData(**results_args))


    # Rewriting the Non CRM units checks (Organics and Tirewear)
    # Check - for Chlorinated Hydrocarbons, PAH, PBDE, PCB, Pyrethroid, TIREWEAR in the sediment matrix, the units must be ng/g dw For non reference material sampletypes
    print('# Check - for Chlorinated Hydrocarbons, PFAS, PBDE, PCB. TIREWEAR in the sediment matrix, the units must be ng/g dw For non reference material sampletypes')
    # (for matrix = sediment)
    ng_over_g_analyteclasses = ['Chlorinated Hydrocarbons', 'PAH', 'PFAS', 'PBDE', 'PCB', 'Pyrethroid', 'TIREWEAR']
    ng_over_g_mask =  (results.analyteclass.isin(ng_over_g_analyteclasses))
    unit_mask = (ng_over_g_mask & (~results.sampletype.str.contains('Reference', case = False))) & (results.units != 'ng/g dw')
    
    results_args.update({
        "badrows": results[unit_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For {','.join(ng_over_g_analyteclasses)} the units must be ng/g dw"
    })
    errs.append(checkData(**results_args))

    # Rewriting the Non CRM units checks (Inorganics)
    # Check - for Inorganics (aka Metals) in the sediment matrix, the units must be ug/g dw For non reference material sampletypes
    print('# Check - for Inorganics (aka Metals) in the sediment matrix, the units must be ug/g dw For non reference material sampletypes')
    # (for matrix = sediment)
    ug_over_g_analyteclasses = ['Inorganics']
    ug_over_g_mask =  (results.analyteclass.isin(ug_over_g_analyteclasses))
    unit_mask = (ug_over_g_mask & (~results.sampletype.str.contains('Reference', case = False))) & (results.units != 'ug/g dw')
    
    results_args.update({
        "badrows": results[unit_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For {(', ').join(ug_over_g_analyteclasses)} the units must be ug/g dw"
    })
    errs.append(checkData(**results_args))
    # -----------------------------------------------------------------------------------------------------------------------------------#

    # -----------------------------------------------------------------------------------------------------------------------------------

    # -----------------------------------------------------------------------------------------------------------------------------------
    # Check - For SampleType/CRM Reference Material and Matrix Sediment, Analyte must have units that match lu_chemcrm. (Error)"
    print("# Check - For SampleType/CRM Reference Material and Matrix Sediment, Analyte must have units that match lu_chemcrm. (Error)")
    crm_analyteclasses = pd.read_sql("SELECT DISTINCT analyteclass FROM lu_chemcrm WHERE matrix = 'sediment'", eng).analyteclass.tolist()
    crmvals = pd.read_sql(
        f"""
        SELECT
            matrix,
            crm as sampletype, 
            units as units_crm, 
            analytename
        FROM lu_chemcrm
        """,
        eng
    )
    checkdf = results.merge(crmvals, on = ['sampletype','analytename','matrix'], how = 'inner')
    badrows = checkdf[checkdf.units != checkdf.units_crm].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For Reference material SampleTypes, units must match those in the reference material document. <a href=/{current_app.script_root}/scraper?action=help&layer=lu_chemcrm target=_blank>See the CRM Lookup list values</a>)"
    })
    errs.append(checkData(**results_args))

    # Check - For sediment matrix, sampletype CRM cannot be a tissue Reference Material
    # Get the tissue CRMs from the lookup list (this way if we ever decide to change their names we dont need to change any code)
    forbidden_crms = pd.read_sql("""SELECT DISTINCT crm FROM lu_chemcrm WHERE matrix = 'tissue';""", eng)
    assert not forbidden_crms.empty, "CRM lookup list lu_chemcrm has no reference materials for the tissue matrix. Check the table in the database, it is likely not configured correctly"
    
    forbidden_crms = forbidden_crms.crm.tolist()
    
    results_args.update({
        "badrows": results[results.sampletype.isin(forbidden_crms)].tmp_row.tolist(),
        "badcolumn": "SampleType",
        "error_type": "Value Error",
        "error_message": f"You are making a sediment chemistry submission but this Reference Material is for Mussel Tissue"
    })
    errs.append(checkData(**results_args))
    # -----------------------------------------------------------------------------------------------------------------------------------
    # ----- END CUSTOM CHECKS - SEDIMENT RESULTS ----- #




    # If there are errors, dont waste time with the QA plan checks
    if errs != []:
        return {'errors': errs, 'warnings': warnings}






    # -=======- BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  
    # Percent Recovery is computed right before the QA Checks since the first stage of custom checks will issue an error for TrueValue as 0. 
    # Calculate percent recovery
    # if truevalue is 0 - critical error: float division by zero BUG
    results['percentrecovery'] = \
        results.apply(
            lambda x: 
            float(x.result)/float(x.truevalue)*100 if ('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower()) else -88, 
            axis = 1
        )

    # ------- Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#
    # 2023-06-01 - Today was the chem technical committee meeting after the 1st chemistry intercalibration round
    # There were no proposed changes to the QA Plan for table 5-3 (metals in sediment)

    # --- TABLE 5-3 Check #0 --- #
    # Check - Frequency checks
    print('# Check - Frequency checks')
    # within the batch, there must be 
    
    
    # --- END TABLE 5-3 Check #0 --- #
    
    # --- TABLE 5-3 Check #1 --- #
    # Check - 15 Analytes must be in each grouping of AnalysisBatchID, SampleID, sampletype, and labreplicate
    # NOTE: Remains the same in bight 2023
    print('# Check - 15 Analytes must be in each grouping of AnalysisBatchID, SampleID, sampletype, and labreplicate')
    #   (if that batch is analyzing inorganics) (ERROR)

    # The filter mask to be used throughout the whole table 5-3 checks
    inorg_sed_mask = (results.analyteclass == 'Inorganics') & results.matrix.isin(['sediment','labwater'])


    # --- END TABLE 5-3 Check #1 --- #
    # Covered above
    if not results[inorg_sed_mask].empty:
        # --- TABLE 5-3 Check #2 --- #
        # Check - For the SampleType "Reference - ERA 540 Sed" - Result should match lu_chemcrm range (PT acceptance limits)
        # NOTE: I need the updated range values to update the lookup table - March 14, 2023 - Robert
        print('# Check - For the SampleType "Reference - ERA 540 Sed" - Result should match lu_chemcrm range (PT acceptance limits)')
        # In my understanding, its mainly for the reference material for inorganics in the sediment matrix, rather than a particular CRM
        # UPDATE - the only CRM for metals in sediment, is ERA 540
        inorg_sed_ref_mask = inorg_sed_mask & (results.sampletype == "Reference - ERA 540 Sed")
        
        crmvals = pd.read_sql(
            f"""
            SELECT 
                analytename, 
                pt_performance_lowerbound AS lower_bound,
                pt_performance_upperbound AS upper_bound
            FROM lu_chemcrm 
            WHERE crm = 'Reference - ERA 540 Sed'
            """,
            eng
        )
        
        checkdf = results[inorg_sed_ref_mask].merge(crmvals, on = 'analytename', how = 'inner')

        badrows = checkdf[
            checkdf.apply(
                lambda row: (row.result < row.lower_bound ) | (row.result > row.upper_bound),
                axis = 1
            )
        ]

        if not badrows.empty:
            badrows = badrows.tmp_row.tolist()
        
            results_args.update({
                "badrows": badrows,
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"The value here is outside the PT performance limits for ERA 540 (<a href=/{current_app.script_root}/scraper?action=help&layer=lu_chemcrm target=_blank>See the CRM Lookup lsit values</a>)"
            })
            warnings.append(checkData(**results_args))
        else:
            badrows = []
        # --- END TABLE 5-3 Check #2 --- #


        # --- TABLE 5-3 Check #3 --- #
        # Check - For Lab blank sampletypes - Result < MDL or Result < 5% of measured concentration in samples (Warning)
        # NOTE: Remains the same in Bight 2023
        print('# Check - For Lab blank sampletypes - Result < MDL or Result < 5% of measured concentration in samples (Warning)')
        argslist = MB_ResultLessThanMDL(results[inorg_sed_mask])
        print("done calling MB ResultLessThanMDL")
        for args in argslist:
            results_args.update(args)
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-3 Check #3 --- #


        # --- TABLE 5-3 Check #4 --- #
        # Sample Duplicate required for 10% of the samples in a batch
        tmp = results[inorg_sed_mask].groupby(['analysisbatchid', 'sampleid','analytename']).apply(
            lambda df: 
            not df[(df.labreplicate == 2) & df.sampletype.isin(['Result'])].empty
        ) 
        if not tmp.empty:    
            tmp = tmp.reset_index(name = 'has_dup')

            # identify samples where not all analytes had their duplicates
            tmp = tmp.groupby(['analysisbatchid','sampleid']).agg({'has_dup': all}).reset_index()
            
            # get percentage of samples within batch that had all analytes with their dupes
            tmp = tmp.groupby('analysisbatchid') \
                .agg({'has_dup': lambda x: sum(x) / len(x)}) \
                .reset_index() \
                .rename(columns = {'has_dup':'percent_samples_with_dupes'})
            
            # batches where 
            badbatches = tmp[tmp.percent_samples_with_dupes < 0.1]

            bad = results[results.analysisbatchid.isin(badbatches.analysisbatchid.tolist())]
            if not bad.empty:
                bad = bad.groupby('analysisbatchid').agg({'tmp_row': list}).reset_index()
                for _, row in bad.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt meet the matrix/sample dup requirement
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"Under 10% of samples in the batch {row.analysisbatchid} have a sample duplicate"
                    })
                    warnings.append(checkData(**results_args))

        # --- END TABLE 5-3 Check #4 --- #


        # --- TABLE 5-3 Check #5 --- #
        # Check - At least one blank spike result per batch should be within 15% of the TrueValue (85 to 115 percent recovery)

        print('# Check - At least one Blank spike result per batch should be within 15% of the TrueValue (85 to 115 percent recovery)')
        # It is checking to see if all analytes in either the blank spike, or the duplicate, were inside of 15% of the TrueValue
        # I need to confirm that this is what it is supposed to do
        
        pct_recovery_thresh = 15
        checkdf = results[inorg_sed_mask & results.sampletype.str.contains('Blank spiked', case = False)] \
            .groupby(['analysisbatchid', 'sampleid','labreplicate']) \
            .apply(
                lambda df: 
                all((df.percentrecovery.between(100 - pct_recovery_thresh, 100 + pct_recovery_thresh)))
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_within15_check')

            # only bad analysis batches will remain
            checkdf = results[inorg_sed_mask & results.sampletype.str.contains('Blank spiked', case = False) ] \
                .merge(checkdf[~checkdf.passed_within15_check], on = 'analysisbatchid', how = 'inner')

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": "Within this analysisbatch, at least one of the Blank spike sets should have had all their percent recoveries within 15 percent"
            })
            warnings.append(checkData(**results_args))
        # --- End Table 5-3 check #5 --- #
        

        # --- TABLE 5-3 Check "#6" (a, b, and c) --- #
        # Check - Duplicate Matrix spikes (or Results) need < 25% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B
        print('# Check - Duplicate Matrix spikes (or Results) need < 25% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B')
        
        # QUESTION - Mercury seems to often be analyzed with the method EPA245.7m - what is the RPD threshold on that?
        # Based on the bight 2018 checker, it looks like Bowen told us it had to be under 30 - thats what the old one does

        # NOTE (March 14, 2023): This will change
        # for 'ICPAES', 'EPA200.7', 'EPA 6010B' it says "10% (within 3 standard deviations)" but lets set it to 25% rpd
        # for 'ICPMS', 'EPA200.8', 'EPA 6020Bm' it says within 25% RPD
        # for 'CVAA','FAA','GFAA','HAA','EPA245.7m','EPA245.5','EPA7473','SW846 7471','EPA7471B' it says within 30% RPD

        # NOTE August 2, 2023
        # Need to make sure these analysis methods match the lookup list
        # Need to make sure that their analysis method makes sense based on the analyteclass (That check should most likely be added)
        # But a much higher priority is to make sure these below analysis methods match the lookup list lu_chemanalysismethods
        
        icpaes_methods = ['ICPAES', 'EPA200.7', 'EPA6010D','EPA6010B'] # methods that require 20% RPD - Inductively Coupled Plasma Atomic Emission Spectrometry
        icpaes_tolerance = .25 
        icpaes_blankspike_tolerance = 0.25
        icpms_methods = ['ICPMS', 'EPA200.8', 'EPA6020'] # methods that require 30% RPD - Inductively Coupled Plasma Mass Spectrometry
        icpms_tolerance = .25 
        icpms_blankspike_tolerance = 0.15
        aa_methods = ['CVAA','CVAF','FAA','GFAA','HAA','EPA245.7','EPA245.5','EPA7473','SW846 7471','EPA7471B'] # - Atomic Absorbtion
        aa_tolerance = .3 

        rpdcheckmask = (
            inorg_sed_mask 
            & (
                results.sampletype.isin(['Matrix spike', 'Result', 'Blank spiked']) 
            )
        )
        checkdf = results[rpdcheckmask]
        checkdf = checkdf.assign(
            tolerance = checkdf.apply( 
                lambda row: 
                icpaes_tolerance
                if ( (row.analysismethod in icpaes_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else icpaes_blankspike_tolerance
                if ( (row.analysismethod in icpaes_methods) and (row.sampletype in ['Blank spiked'] ) ) 
                else icpms_tolerance
                if ( (row.analysismethod in icpms_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else icpms_blankspike_tolerance
                if ( (row.analysismethod in icpms_methods) and (row.sampletype in ['Blank spiked'] ) ) 
                else aa_tolerance
                if ( (row.analysismethod in aa_methods) and (row.sampletype in ['Result','Matrix spike'] ) ) 
                else pd.NA
                ,
                axis = 1
            ),
            analysismethodgroup = checkdf.analysismethod.apply( 
                lambda x: 'ICPAES' if x in icpaes_methods else 'ICPMS' if x in icpms_methods else 'AA'
            )
        )

        # drop records where the tolerance ended up as pd.NA
        checkdf.dropna(subset = 'tolerance', inplace = True)

        checkdf = checkdf.groupby(['analysisbatchid', 'analysismethod', 'analysismethodgroup', 'sampletype', 'analytename','sampleid', 'tolerance']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2))
        )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd')
            checkdf['errmsg'] = checkdf.apply(
                lambda row:
                (
                    f"For the AnalysisMethod {row.analysismethod}, "
                    f"{'Matrix spike' if row.sampletype == 'Matrix spike' else 'Blank spike' if row.sampletype == 'Blank spiked' else 'Sample'}"
                    f" duplicates should have an RPD under {(row.tolerance) * 100}%"
                )
                , axis = 1
            )
            checkdf = results[rpdcheckmask] \
                .merge(
                    checkdf[
                        # just merge records that failed the check
                        # We never multiplied RPD by 100, so it should be expressed as a decimal here
                        checkdf.apply(lambda x: x.rpd > x.tolerance, axis = 1)
                    ], 
                    on = ['analysisbatchid','analysismethod','sampletype','analytename','sampleid'], 
                    how = 'inner'
                )
            
            tmp = checkdf.groupby(['errmsg']) \
                .apply(lambda df: df.tmp_row.tolist())
            if not tmp.empty:
                argslist = tmp \
                    .reset_index(name = 'badrows') \
                    .apply(
                        lambda row: 
                        {
                            "badrows": row.badrows,
                            "badcolumn": "Result",
                            "error_type": "Value Error",
                            "error_message": row.errmsg
                        },
                        axis = 1
                    ).tolist()

                for args in argslist:
                    results_args.update(args)
                    warnings.append(checkData(**results_args))


            
        # --- END TABLE 5-3 Check --- # (# Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B)
        print("# --- END TABLE 5-3 Check --- # (# Check - Duplicate Matrix spikes (or Results) need < 20% RPD for AnalysisMethods ICPAES, EPA200.7 and EPA 6010B)")
        
        
        # --- TABLE 5-3 Check --- #
        # --- Table 5-3 - AnalysisMethods ICPAES and ICPMS, blank spike duplicates are required --- #
        print("# --- Table 5-3 - AnalysisMethods ICPAES and ICPMS, blank spike duplicates are required --- #")
        tmp_orig = results[inorg_sed_mask & results.analysismethod.isin([*icpaes_methods, *icpms_methods])] 
        tmp = tmp_orig.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a blank spiked duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_blankspike_dup') 
            tmp = tmp[~tmp.has_blankspike_dup] # get batches without the blank spike dupes
            tmp = tmp_orig.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a blank spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename} (since it is a batch for metals with analysismethod ICPAES or ICPMS)"
                    })
                    warnings.append(checkData(**results_args))




        # ------- END Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#
        print("# ------- END Table 5-3 - Inorganics, Non-tissue matrices (Sediment and labwater) -------#")




    # ------- Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#
    # 2023-06-01 - Today was the chem technical committee meeting after the 1st chemistry intercalibration round
    # For organics in sediment, it seems like analyteclasses will not be separated, but that is something to hold off on
    # It looks like for all organics in sediment, there will be the same data quality objectives
    # Also as of today they are doing away with the blank spike and blank spike duplicate requirements, and instead going with Matrix spikes

    print("# ------- Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#")
    # The filter mask to be used throughout the whole table 5-4 checks
    pah_sed_mask = (results.analyteclass == 'PAH') & results.matrix.isin(['sediment','labwater'])


    if not results[pah_sed_mask].empty:
        # --- TABLE 5-4 Check #1 --- #
        # Check - Make sure they have all the required PAH anlaytes
        print('# Check - Make sure they have all the required PAH anlaytes')
    
        # 24 required analytes from the PAH analyteclass
        req_analytes_tbl54 = pd.read_sql("SELECT * FROM lu_analytes WHERE analyteclass = 'PAH'", eng).analyte.tolist()

        # --- END TABLE 5-4 Check #1 --- #
        # Covered above, except it doesnt specifically tell which reference materials it requires
        checkdf = results[pah_sed_mask]
        if not checkdf.empty:
            
            # Go through each batch and analyte and see which reference materials values are missing
            tmp = checkdf.groupby(['analysisbatchid','analytename']).agg({
                'sampletype': ( lambda sampletypes: len(set(['Reference - SRM 1944 Sed','Reference - SRM 1941b Sed']).intersection(set(sampletypes))) > 0),
                'tmp_row': list
            }) 

            # you can never be too careful
            if not tmp.empty:
                tmp = tmp.reset_index().rename(columns={'sampletype':'has_correct_crm'})
                print('tmp')
                print(tmp)
                tmp = tmp[~tmp.has_correct_crm].apply(
                    lambda row: 
                    {
                        "badrows": row.tmp_row, # The whole thing is bad since it is missing Reference Material
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"For PAHs in sediment, it is required that you have one of the following reference materials: 'Reference - SRM 1944 Sed' or 'Reference - SRM 1941b Sed' but it is missing for the batch {row.analysisbatchid} and analyte {row.analytename}"
                    },
                    axis = 1
                )
                if not tmp.empty:
                    tmp = tmp.tolist()
                    for argset in tmp:
                        results_args.update(argset)
                        errs.append(checkData(**results_args))



        # --- TABLE 5-4 Check #2 --- #
        # Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes
        # print('# Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes')
        crmvals = pd.read_sql(
            f"""
            SELECT crm AS sampletype, analytename, reference_value FROM lu_chemcrm 
            WHERE analytename IN ('{"','".join(req_analytes_tbl54).replace(';','')}')
            AND matrix = 'sediment'
            """,
            eng
        )
        checkdf = results[pah_sed_mask & results.sampletype.str.contains('Reference', case = False)] 
        if not checkdf.empty:
            checkdf = checkdf.merge(crmvals, on = ['sampletype','analytename'], how = 'inner')
            
            if not checkdf.empty:
                print("checkdf")
                print(checkdf)
                checkdf['within40pct'] = checkdf.apply(
                        lambda row:
                        (0.6 * float(row.reference_value)) <= row.result <= (1.4 * float(row.reference_value)) if not pd.isnull(row.reference_value) else True,
                        axis = 1
                    )
                checkdf = checkdf.merge(
                    checkdf.groupby('analysisbatchid') \
                        .apply(
                            lambda df: (sum(df.within40pct) / len(df)) < 0.8
                        ) \
                        .reset_index(name = 'failedcheck'),
                    on = 'analysisbatchid',
                    how = 'inner'
                )
                checkdf = checkdf[checkdf.failedcheck]
                results_args.update({
                    "badrows": checkdf.tmp_row.tolist(),
                    "badcolumn": "AnalysisBatchID",
                    "error_type": "Value Error",
                    "error_message": "Less than 80% of the analytes in this batch are within 40% of the CRM value"
                })
                warnings.append(checkData(**results_args))

        # --- END TABLE 5-4 Check #2 --- #
        print("# --- END TABLE 5-4 Check #2 --- #")
        
        

        # --- TABLE 5-4 Check #3 --- #
        # Check - Matrix spike duplicate required (1 per batch)
        print('# Check - Matrix spike duplicate required (1 per batch)')
        tmp_orig = results[pah_sed_mask] 
        tmp = tmp_orig.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
            tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
            tmp = tmp_orig.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a matrix spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #3 --- #
        print("# --- END TABLE 5-4 Check #3 --- #")


        


        # --- TABLE 5-4 Check #4 --- #
        # Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes
        print('# Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes')
        checkdf = results[pah_sed_mask & results.sampletype.str.contains('Matrix spike', case = False)]
        checkdf = checkdf.groupby(['analysisbatchid', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.4
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_40')
            checkdf = checkdf.groupby('analysisbatchid').apply(lambda df: sum(df.rpd_under_40) / len(df) >= 0.7 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Matrix spikes should have an RPD under 40% for 70% of the analytes in the batch"
                    , axis = 1
                )
                checkdf = results[pah_sed_mask & results.sampletype.str.contains('Matrix spike', case = False)] \
                    .merge(checkdf[~checkdf.passed], on = ['analysisbatchid'], how = 'inner')
                
                argslist = checkdf.groupby(['errmsg']) \
                    .apply(lambda df: df.tmp_row.tolist())
                
                if not argslist.empty:
                    argslist = argslist \
                        .reset_index(name = 'badrows') \
                        .apply(
                            lambda row: 
                            {
                                "badrows": row.badrows,
                                "badcolumn": "Result",
                                "error_type": "Value Error",
                                "error_message": row.errmsg
                            },
                            axis = 1
                        ).tolist()

                    for args in argslist:
                        results_args.update(args)
                        warnings.append(checkData(**results_args))

        # --- END TABLE 5-4 Check #4 --- #
        print("# --- END TABLE 5-4 Check #4 --- #")
        
        
        # --- TABLE 5-4 Check #5 and 6 --- #
        # 2023-06-01 - Chem Technical committee relaxed restriction, saying that the 60 to 140% recovery should be met for 70% or more of analytes (before, it was 80%)
        # Check - within an analysisbatch, Matrix spikes should have 60-140% recovery of spiked mass for 70% of analytes
        print('# Check - within an analysisbatch, Matrix spikes should have 60-140% recovery of spiked mass for 70% of analytes')
        checkdf = results[pah_sed_mask & results.sampletype.isin(['Matrix spike'])] \
            .groupby(['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate']) \
            .apply(
                lambda df: 
                (sum((df.percentrecovery > 60) & (df.percentrecovery < 140)) / len(df)) >= 0.7
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Matrix spike'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 60) | (checkdf.percentrecovery > 140))]

            # changed sampleid to labsamplid inside badcolumns --- TEST
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Matrix spikes (for organics in sediment), more than 70% of analytes should have 60-140% recovery"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #5 --- #
        print("# --- END TABLE 5-4 Check #5 --- #")



        # --- TABLE 5-4 Check #7 --- #
        # Check - For SampleType = Lab blank, we must require Result < 10 * MDL - if that criteria is met, the qualifier should be "none"
        print('# Check - For SampleType = Lab blank, we must require Result < 10 * MDL - if that criteria is met, the qualifier should be "none"')
        
        # First check that the result is under 10 times the MDL
        badrows = results[(pah_sed_mask & (results.sampletype == 'Lab blank')) & (results.result >= (10 * results.mdl))].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, the result must be less than 10 times the MDL (for PAH)"
        })
        warnings.append(checkData(**results_args))

        # If the requirement is met, check that the qualifier says none
        badrows = results[
            ((pah_sed_mask & results.sampletype == 'Lab blank') & (results.result < (10 * results.mdl))) & 
            (results.qualifier != 'none')
        ].tmp_row.tolist()

        results_args.update({
            "badrows": badrows,
            "badcolumn": "Qualifier",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, if the result is less than 10 times the MDL, then the qualifier should say 'none' (for PAH)"
        })
        warnings.append(checkData(**results_args))
        # --- END TABLE 5-4 Check #7 --- #


        # --- TABLE 5-4 Check # --- #
        # Check - 
        print('# Check - ')
        # --- END TABLE 5-4 Check # --- #

    # ------- END Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- END Table 5-4 - PAH, Non-tissue matrices (Sediment and labwater) -------#")




    # ------- Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#")

    analyteclasses55 = ['PCB','PBDE','Chlorinated Hydrocarbons','Pyrethroid','Neonicotinoids','PFAS','TIREWEAR']
    mask55 = results.analyteclass.isin(analyteclasses55)
    results55 = results[mask55]
    
    print("results55")
    print(results55)
    if not results55.empty:

        print("# --- TABLE 5-5 Check #1 --- #")
        # --- TABLE 5-5 Check #1 --- #
        # Check - check for all required sampletypes
        # Covered above, except it doesnt specifically tell which reference materials it requires
        checkdf = results55[results55.analyteclass.isin(['PCB', 'PBDE', 'Chlorinated Hydrocarbons'])]
        if not checkdf.empty:
            
            # Go through each batch and analyte and see which reference materials values are missing
            tmp = checkdf.groupby(['analysisbatchid','analyteclass']).agg({
                'sampletype': ( lambda sampletypes: len(set(['Reference - SRM 1944 Sed','Reference - SRM 1941b Sed']).intersection(set(sampletypes))) > 0),
                'tmp_row': list
            }) 

            # you can never be too careful
            if not tmp.empty:
                tmp = tmp.reset_index().rename(columns={'sampletype':'has_correct_crm'})
                print('tmp')
                print(tmp)
                tmp = tmp[~tmp.has_correct_crm].apply(
                    lambda row: 
                    {
                        "badrows": row.tmp_row, # The whole thing is bad since it is missing Reference Material
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"For Organics in sediment, it is required that you have one of the following reference materials: 'Reference - SRM 1944 Sed' or 'Reference - SRM 1941b Sed' but it is missing for the batch {row.analysisbatchid} and compound class {row.analyteclass}"
                    },
                    axis = 1
                )
                if not tmp.empty:
                    tmp = tmp.tolist()
                    for argset in tmp:
                        results_args.update(argset)
                        errs.append(checkData(**results_args))

        print("# --- END TABLE 5-5 Check #1 --- #")
        # --- END TABLE 5-5 Check #1 --- #
        

        # --- TABLE 5-5 Check #2 --- #
        # 2023-06-01 Change to QA plan from chem technical committee - CRM accuracy must be met for 80% of more of analytes in the class (up from 70%)
        # Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes
        print('# Check - For reference materials - Result should be within 40% of the specified value (in lu_chemcrm) for 80% of the analytes')
        crmvals = pd.read_sql(
            f"""
            SELECT
                lu_chemcrm.crm AS sampletype,
                lu_chemcrm.analytename,
                lu_chemcrm.matrix,
                lu_chemcrm.certified_value,
                lu_analytes.analyteclass 
            FROM
                lu_chemcrm
                JOIN lu_analytes ON lu_chemcrm.analytename = lu_analytes.analyte 
            WHERE
                lu_analytes.analyteclass IN ( '{"','".join(analyteclasses55)}' ) 
                AND matrix = 'sediment'
            """,
            eng
        )
        checkdf = results[mask55 & results.sampletype.str.contains('Reference', case = False)]
        if not checkdf.empty:
            checkdf = checkdf.merge(crmvals, on = ['sampletype','analytename','matrix'], how = 'left')
        
        if not checkdf.empty:
            checkdf['within40pct'] = checkdf.apply(
                    lambda row:
                    (0.6 * float(row.certified_value)) <= row.result <= (1.4 * float(row.certified_value)) if not pd.isnull(row.certified_value) else True
                    ,axis = 1
                )
            checkdf = checkdf.merge(
                checkdf.groupby('analysisbatchid') \
                    .apply(
                        lambda df: (sum(df.within40pct) / len(df) )< 0.8
                    ) \
                    .reset_index(name = 'failedcheck'),
                on = 'analysisbatchid',
                how = 'inner'
            )
            checkdf = checkdf[checkdf.failedcheck]
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID",
                "error_type": "Value Error",
                "error_message": "Less than 80% of the analytes in this batch are within 40% of the CRM value"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #2 --- #
        print("# --- END TABLE 5-5 Check #2 --- #")

        # --- TABLE 5-5 Check #3, #6 --- #
        # Check - Matrix spike duplicate required (1 per batch)
        print('# Check #3 - Matrix spike duplicate required (1 per batch)')
        tmp = results55.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
            tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
            tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a matrix spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        
        #(Check #6, sample as check #3 except with Blank spikes)
        print('# Check #6 - Blank spike duplicate required (1 per batch)')
        print('# Check #6 Temporariliy disabled due to a modification to QA plan (2023-06-01 after Chem technical committee meeting)')
        # tmp = results55.groupby(['analysisbatchid', 'analytename']).apply(
        #     lambda df:
        #     not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        # )
        # if not tmp.empty:
        #     tmp = tmp.reset_index( name = 'has_blankspike_dup') 
        #     tmp = tmp[~tmp.has_blankspike_dup] # get batches without the matrix spike dupes
        #     tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
        #     tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
        #     if not tmp.empty:
        #         tmp = tmp.reset_index()
        #         for _, row in tmp.iterrows():
        #             results_args.update({
        #                 "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
        #                 "badcolumn": "SampleType",
        #                 "error_type": "Incomplete data",
        #                 "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename}"
        #             })
        #             warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #6 --- #
        print("# --- END TABLE 5-5 Check #6 --- #")


        # --- TABLE 5-5 Check #4, #7 --- #
        # Check - Within an analysisbatch, Matrix spikes/Blank spikes should have 60-140% recovery of spiked mass for 70% of analytes (WARNING)
        print('# Check # 4 and 7 - Within an analysisbatch, Matrix spikes/Blank spikes should have 60-140% recovery of spiked mass for 70% of analytes (WARNING)')
        print("Check # 7 temporarily disabled due to modification to QA plan on 2023-06-01 by chem technical committee")
        checkdf = results[mask55 & results.sampletype.isin(['Matrix spike'])]
        if not checkdf.empty:
            checkdf = checkdf.groupby(['analysisbatchid', 'sampletype', 'analyteclass','sampleid','labreplicate']) \
                .apply(
                    lambda df: 
                    (sum((df.percentrecovery > 60) & (df.percentrecovery < 140)) / len(df)) >= 0.7
                )
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'analyteclass','sampleid','labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Matrix spike'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 60) | (checkdf.percentrecovery > 140))]

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Matrix spikes, over 70% of analytes should have 60-140% recovery"
            })
            warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check # 4 and 7 --- #
        print("# --- END TABLE 5-5 Check # 4 and 7 --- #")
        
        # --- TABLE 5-5 Check #5, #8 --- #
        # Check - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes 
        # NOTE (used to be RPD < 30% - up to 40% on 2023-06-01 based on the meeting after 1st round of intercal)
        print('# Check #5, #8 - Duplicate Matrix spikes must have RPD < 40% for 70% of the analytes')
        print("Check #8 (about the Blank spike duplicate RPD) temporarily disabled due to a change to the QA plan on 2023-06-01 from Chem technical committee")

        checkdf = results[mask55 & results.sampletype.isin(['Matrix spike'])]
        checkdf = checkdf.groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.4
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_40')
            checkdf = checkdf.groupby(['analysisbatchid','analyteclass']).apply(lambda df: sum(df.rpd_under_40) / len(df) >= 0.7 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Matrix spikes should have an RPD under 40% for 70% of the analytes in the batch ({row.analysisbatchid}) (for the analyteclass {row.analyteclass})"
                    , axis = 1
                )
                checkdf = results[mask55 & results.sampletype.isin(['Matrix spike'])] \
                    .merge(checkdf[~checkdf.passed], on = ['analysisbatchid', 'analyteclass'], how = 'inner')
                
                if not checkdf.empty:
                    argslist = checkdf.groupby(['errmsg']) \
                        .apply(lambda df: df.tmp_row.tolist()) \
                        .reset_index(name = 'badrows') \
                        .apply(
                            lambda row: 
                            {
                                "badrows": row.badrows,
                                "badcolumn": "Result",
                                "error_type": "Value Error",
                                "error_message": row.errmsg
                            },
                            axis = 1
                        ).tolist()

                    for args in argslist:
                        results_args.update(args)
                        warnings.append(checkData(**results_args))
        # --- END TABLE 5-5 Check #5 and #8 --- #
        print("# --- END TABLE 5-5 Check #5 and #8 --- #")



        # --- TABLE 5-5 Check #9 --- #
        # Check - For Lab Blanks, result has to be less than 10 * MDL and the Result must be less than the RL (WARNING)
        print('# Check - For Lab Blanks, result has to be less than 10 * MDL and the Result must be less than the RL (WARNING)')
        #   if that criteria is met then the qualifier should be 'none'

        tmpdf = results55[ results55.sampletype.isin(['Lab blank', 'Equipment blank', 'Field blank']) ]
        badrows = tmpdf[(tmpdf.result >= (10 * tmpdf.mdl)) | (tmpdf.result >= tmpdf.rl)].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": "For Lab blanks, Equipment blanks, and Field blanks, the result should be less than 10 times the MDL, and less than the RL"
        })
        warnings.append(checkData(**results_args))
        
        # Second part of the check - if the criteria is met then the qualifier should be "none"
        badrows = tmpdf[((tmpdf.result < 10 * tmpdf.mdl) & (tmpdf.result < tmpdf.rl)) & (tmpdf.qualifier.str.lower() != 'none')].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Qualifier",
            "error_type": "Value Error",
            "error_message": "For Lab blanks, Equipment blanks, and Field blanks (for PCB, PBDE, CHCs, PFAS, Neonics, Tirewear) if the result is less than 10 times the MDL, and less than the RL, then the qualifer should say 'none'"
        })
        errs.append(checkData(**results_args))

        # --- END TABLE 5-5 Check #9 --- #
        print("# --- END TABLE 5-5 Check #9 --- #")

        # --- TABLE 5-5 Check # --- #
        # Check - 
        print('# Check - ')
        # --- END TABLE 5-5 Check # --- #
    
    
    # ------- END Table 5-5 - Pyrethroids, PCB, PBDE, Chlorinated Hydrocarbons, Non-tissue matrices (Sediment and labwater) -------#
    
    
    # ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#

    results56 = results[results.analyteclass.isin(['TOC','TN'])]
    
    # --- TABLE 5-6 Check #1 --- #
    # Check for all required sampletypes (covered above)
    if not results56.empty:

        print("# --- TABLE 5-5 Check #1 --- #")
        # --- TABLE 5-5 Check #1 --- #
        # Check - check for all required sampletypes
        # Covered above, except it doesnt specifically tell which reference materials it requires
        checkdf = results55[results55.analyteclass.isin(['PCB', 'PBDE', 'Chlorinated Hydrocarbons'])]
        if not checkdf.empty:
            
            # Go through each batch and analyte and see which reference materials values are missing
            tmp = checkdf.groupby(['analysisbatchid','analyteclass']).agg({
                'sampletype': ( lambda sampletypes: len(set(['Reference - SRM 1944 Sed','Reference - SRM 1941b Sed']).intersection(set(sampletypes))) > 0),
                'tmp_row': list
            }) 

            # you can never be too careful
            if not tmp.empty:
                tmp = tmp.reset_index().rename(columns={'sampletype':'has_correct_crm'})
                print('tmp')
                print(tmp)
                tmp = tmp[~tmp.has_correct_crm].apply(
                    lambda row: 
                    {
                        "badrows": row.tmp_row, # The whole thing is bad since it is missing Reference Material
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"For Organics in sediment, it is required that you have one of the following reference materials: 'Reference - SRM 1944 Sed' or 'Reference - SRM 1941b Sed' but it is missing for the batch {row.analysisbatchid} and compound class {row.analyteclass}"
                    },
                    axis = 1
                )
                if not tmp.empty:
                    tmp = tmp.tolist()
                    for argset in tmp:
                        results_args.update(argset)
                        errs.append(checkData(**results_args))

        print("# --- END TABLE 5-5 Check #1 --- #")
        # --- END TABLE 5-5 Check #1 --- #
    # --- END TABLE 5-6 Check #1 --- #


    # --- TABLE 5-6 Check #2 --- #
    # Check if the value is within 20% of the CRM values (for the reference materials)
    print("# Check if the value is within 20% of the CRM values (for the reference materials)")
    
    # crmvals dataframe has been defined above, in section 5-3
    if not results56.empty:
        checkdf = results56[(results56.analytename == 'TOC') & (results56.sampletype == 'Reference - SRM 1944 Sed')]
        if not checkdf.empty:
            crmvals = pd.read_sql(
                f"""
                SELECT 
                    crm AS sampletype,
                    matrix,
                    analytename,
                    certified_value
                FROM lu_chemcrm 
                WHERE 
                    crm IN ('Reference - SRM 1944 Sed', 'Reference - SRM 1941b Sed')
                    AND analytename = 'TOC'
                """,
                eng
            )
            checkdf = checkdf.merge(crmvals, on = ['matrix','sampletype','analytename'], how = 'left') 
            checkdf = checkdf.assign(failedcheck = ((checkdf.certified_value * 0.8 > checkdf.result) | (checkdf.result > checkdf.certified_value * 1.2)))

            checkdf = checkdf[checkdf.failedcheck]
            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"The result should be within 20% of the certified value in <a target=_blank href=/{current_app.config.get('APP_SCRIPT_ROOT')}/scraper?action=help&layer=lu_chemcrm>lu_chemcrm</a>"
            })
            warnings.append(checkData(**results_args))

    # --- END TABLE 5-6 Check #2 --- #
    print("# --- END TABLE 5-6 Check #2 --- #")



    # --- TABLE 5-6 Check #3 --- #
    # Check - For SampleType = Lab blank, we must require Result < 10 * MDL (WARNING)
    print('# Check - For SampleType = Lab blank, we must require Result < 10 * MDL (WARNING)')
    #   if that criteria is met, the qualifier should be "none" (WARNING)
    # First check that the result is under 10 times the MDL
    if not results56.empty:
        badrows = results56[
            ((results56.analyteclass.isin(['TOC','TN'])) & (results56.sampletype == 'Lab blank')) & (results56.result >= (10 * results56.mdl))
        ].tmp_row.tolist()
        results_args.update({
            "badrows": badrows,
            "badcolumn": "Result",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, the result must be less than 10 times the MDL (for TOC and TN)"
        })
        warnings.append(checkData(**results_args))

        # If the requirement is met, check that the qualifier says none
        badrows = results56[
            (((results56.analyteclass.isin(['TOC','TN'])) & (results56.sampletype == 'Lab blank')) & (results56.result < (10 * results56.mdl)))
            & 
            (results56.qualifier != 'none')
        ].tmp_row.tolist()

        results_args.update({
            "badrows": badrows,
            "badcolumn": "Qualifier",
            "error_type": "Value Error",
            "error_message": f"For Lab blanks, if the result is less than 10 times the MDL, then the qualifier should say 'none' (for TOC and TN)"
        })
        warnings.append(checkData(**results_args))
    
    # --- END TABLE 5-6 Check #3 --- #

    # --- Table 5-6 Check #4 --- #
    print('# Check - Sample duplicate required (1 per batch)')
    if not results56.empty:
        tmp = results56.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Result') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_sample_dup') 
            tmp = tmp[~tmp.has_sample_dup] # get batches without the matrix spike dupes
            tmp = results55.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a sample dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a sample duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
    # --- END Table 5-6 Check #4 --- #

    # --- TABLE 5-6 Check #5 --- #
    # Check - Duplicate Results must have RPD < 30% (WARNING)
    print('# Check - Duplicate Results must have RPD < 30% (WARNING)')
    if not results56.empty:
        checkdf = results56[results56.analyteclass.isin(['TOC','TN']) & (results56.sampletype == 'Result')]
        if not checkdf.empty:
            checkdf = checkdf.groupby(['analysisbatchid', 'analytename','sampleid']).apply(
                lambda subdf:
                abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2))
            )
            if not checkdf.empty:
                #checkdf = checkdf.reset_index(name = 'rpd')
                checkdf = checkdf.reset_index()
                checkdf = checkdf.rename(columns = {0:'rpd'})
                print("checkdf has rpd column")
                print(checkdf)
    
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Sample duplicates should have an RPD under 30% (for TOC and TN)"
                    , axis = 1
                )
                checkdf = results56[results56.analyteclass.isin(['TOC','TN']) & (results56.sampletype == 'Result')] \
                    .merge(
                        checkdf[
                            # just merge records that failed the check
                            checkdf.rpd.apply(lambda x: x >= .30)
                        ], 
                        on = ['analysisbatchid','analytename','sampleid'], 
                        how = 'inner'
                    )
                if not checkdf.empty:
                    argslist = checkdf.groupby(['errmsg']) \
                        .apply(lambda df: df.tmp_row.tolist()) \
                        .reset_index() \
                        .rename(columns = {0:'badrows'}) \
                        .apply(
                            lambda row: 
                            {
                                "badrows": row.badrows,
                                "badcolumn": "Result",
                                "error_type": "Value Error",
                                "error_message": row.errmsg
                            },
                            axis = 1
                        ).tolist()
                    for args in argslist:
                        results_args.update(args)
                        warnings.append(checkData(**results_args))
        
    # --- END TABLE 5-6 Check #5 --- #



    # ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#
    print("# ------- END Table 5-6 - TOC and TN, Non-tissue matrices (Sediment and labwater) -------#")


    # -=======- END BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  
    
    return {'errors': errs, 'warnings': warnings}
