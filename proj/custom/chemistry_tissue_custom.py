# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData
from .chem_functions_custom import *
import pandas as pd

def chemistry_tissue(all_dfs):
    
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
    results = all_dfs['tbl_chemresults_tissue']

    batch['tmp_row'] = batch.index
    results['tmp_row'] = results.index

    # Tack on analyteclass
    results = results.merge(
        pd.read_sql("""SELECT analyte AS analytename, analyteclass FROM lu_analytes""", eng),
        on = 'analytename',
        how = 'inner'
    )

    # Calculate percent recovery
    results['percentrecovery'] = \
        results.apply(
            lambda x: 
            float(x.result)/float(x.truevalue)*100 if ('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower()) else -88, 
            axis = 1
        )
    
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
        "tablename": 'tbl_chemresults_tissue',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # ----- LOGIC CHECKS ----- # 
    print('# ----- LOGIC CHECKS ----- # ')
    # Batch and Results must have matching records on Lab, PreparationBatchID and SampleID

    # check records that are in batch but not in results
    # checkLogic function will update the arguments
    # Check for records in batch but not results
    print("batch")
    print(batch)
    print("results")
    print(results)
    badrows = batch[~batch[['lab','preparationbatchid']].isin(results[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    batch_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Batch must have a matching record in Chemistry Results. Records are matched on Lab and PreparationID."
    })
    errs.append(checkData(**batch_args))

    # Check for records in results but not batch
    badrows = results[~results[['lab','preparationbatchid']].isin(batch[['lab','preparationbatchid']].to_dict(orient='list')).all(axis=1)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Lab, PreparationBatchID",
        "error_type": "Logic Error",
        "is_core_error": False,
        "error_message": "Each record in Chemistry Results must have a matching record in Chemistry Batch. Records are matched on Lab and PreparationID."
    })
    errs.append(checkData(**results_args))


    # - - - - - - - - - - Sample Assignment Check - - - - - - - - - - #
    print("# - - - - - - - - - - Sample Assignment Check - - - - - - - - - - #")
    # # commented out since tbl_chemresults_tissue DOES NOT have a stationid column - zaib 2june2023
    # # Sample Assignment check - make sure they were assigned the analyteclasses that they are submitting
    # badrows = sample_assignment_check(eng = eng, df = results, parameter_column = 'analyteclass')
    # 
    # The above sample assignment check will not work for tissue chemistry
    # The assignments are done via the bioaccumulation sampleid lookup list - Robert August 2, 2024

    print("# Get sample assignments from lu_bioaccumulationsampleid")
    # Get sample assignments from lu_bioaccumulationsampleid
    sample_assignment = pd.read_sql("SELECT sampleid AS bioaccumulationsampleid, legacy_contaminant_lab, pfas_lab FROM lu_bioaccumulationsampleid", eng)

    print("# tack on sample assignments")
    # tack on sample assignments
    tmp = results \
        .merge(sample_assignment, on = 'bioaccumulationsampleid', how = 'left')

    print("# Get where PFAS was messed up")
    # Get where PFAS was messed up
    # Exclude the 0000 QA sampleid placeholder
    pfas_badrows = tmp[
        (tmp.analyteclass.str.upper() == 'PFAS') & (tmp.pfas_lab.str.lower() != tmp.lab.str.lower())
        & (tmp.bioaccumulationsampleid.astype(str) != '0000')
    ].tmp_row.tolist()

    print("# Get where any other contaminant was messed up")
    # Get where any other contaminant was messed up
    # Exclude the 0000 QA sampleid placeholder
    legacy_contaminant_badrows = tmp[
        (tmp.analyteclass.str.upper() != 'PFAS') & (tmp.legacy_contaminant_lab.str.lower() != tmp.lab.str.lower())
        & (tmp.bioaccumulationsampleid.astype(str) != '0000')
    ].tmp_row.tolist()

    print("# Flag if they submitted PFAS but were not assigned")
    # Flag if they submitted PFAS but were not assigned
    results_args.update({
        "badrows": pfas_badrows,
        "badcolumn": "BioaccumulationSampleID,Lab,AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"Your lab was not assigned to submit PFAS data from this sample ID (<a href=scraper?action=help&layer=lu_bioaccumulationsampleid target=_blank>see the sample assignments lookup list</a>)"
    })
    warnings.append(checkData(**results_args))

    print("# Flag if they submitted Legacy Contaminants but were not assigned")
    # Flag if they submitted Legacy Contaminants but were not assigned
    results_args.update({
        "badrows": legacy_contaminant_badrows,
        "badcolumn": "BioaccumulationSampleID,Lab,AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"Your lab was not assigned to submit legacy contaminant data (Non-PFAS) from this sample ID (<a href=scraper?action=help&layer=lu_bioaccumulationsampleid target=_blank>see the sample assignments lookup list</a>)"
    })
    warnings.append(checkData(**results_args))

    print("# - - - - - - - - - - END Sample Assignment Check - - - - - - - - - - #")
    # - - - - - - - - - - END Sample Assignment Check - - - - - - - - - - #




    # Check - A tissue chemistry submission cannot have records with a matrix of "sediment"
    results_args.update({
        "badrows": results[results.matrix == 'sediment'].tmp_row.tolist(),
        "badcolumn": "Matrix",
        "error_type": "Logic Error",
        "error_message": f"This is a tissue chemistry submission but this record has a matrix value of 'sediment'"
    })
    errs.append(checkData(**results_args))

    # ----- END LOGIC CHECKS ----- # 
    print('# ----- END LOGIC CHECKS ----- # ')

    # ----- CUSTOM CHECKS - TISSUE RESULTS ----- #
    print('# ----- CUSTOM CHECKS - TISSUE RESULTS ----- #')

    # Check for All required analytes per bioaccumulationsampleid (All or nothing)
    current_matrix = 'tissue' #sediment or tissue - affects query for required analytes
    # sediment uses stationid, but tissue uses bioaccumulationsampleid (in place of stationid)
    # Check for all required analytes per bioaccumulationsampleid - if a bioaccumulationsampleid has a certain analyteclass
    req_anlts = pd.read_sql(f"SELECT analyte AS analytename, analyteclass FROM lu_analytes WHERE b23{current_matrix}='yes'", eng) \
        .groupby('analyteclass')['analytename'] \
        .apply(set) \
        .to_dict()

    
    chkdf = results.groupby(['bioaccumulationsampleid','sampletype','analyteclass'])['analytename'].apply(set).reset_index()
    print("chkdf")
    print(chkdf)
    chkdf['missing_analytes'] = chkdf.apply(
        lambda row: ', '.join(list((req_anlts.get(row.analyteclass) if req_anlts.get(row.analyteclass) is not None else set()) - row.analytename)), axis = 1 
    )

    chkdf = chkdf[chkdf.missing_analytes != set()]
    print("chkdf")
    print(chkdf)

    # Reference materials will not always have all the analytes
    # labs may enter -88 for those analytes which dont have reference material values
    if not chkdf.empty:
        print("inside if chkdf not empty")
        chkdf = results.merge(chkdf[chkdf.missing_analytes != ''], how = 'inner', on = ['bioaccumulationsampleid','sampletype','analyteclass'])
        print("chkdf")
        print(chkdf)
        chkdf = chkdf.groupby(['bioaccumulationsampleid','sampletype','analyteclass','missing_analytes']).agg({'tmp_row': list}).reset_index()
        errs_args = chkdf.apply(
            lambda row:
            {
                "error_or_warning": "warning" if ('Reference' in str(row.sampletype)) else "error",
                "badrows": row.tmp_row,
                "badcolumn" : "BioAccumulationSampleID,SampleType",
                "error_type": "missing_data",
                "error_message" : f"For the BioAccumulationSampleID {row.bioaccumulationsampleid} and sampletype {row.sampletype}, you attempted to submit {row.analyteclass} but are missing some required analytes ({row.missing_analytes})"
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
    print("# End of checking all required analytes per station, if they attempted submission of an analyteclass")
    # End of checking all required analytes per station, if they attempted submission of an analyteclass
    # No partial submissions of analyteclasses
   
    # ------------------------- Begin chemistry base checks ----------------------------- #
    # ----- CUSTOM CHECKS - TISSUE RESULTS ----- #
    print('# ----- CUSTOM CHECKS - TISSUE RESULTS ----- #')

    # Check for duplicates on bioaccumulationsampleid, sampledate, analysisbatchid, sampletype, matrix, analytename, labreplicate, SAMPLEID
    # Cant be done in Core since there is no sampleid column that we are having them submit, but rather it is a field we create internally based off the labsampleid column
    dupcols = ['bioaccumulationsampleid', 'sampledate', 'analysisbatchid', 'sampletype', 'matrix', 'analytename', 'labreplicate', 'sampleid']
    
    # Technically doing sort_values is unnecessary and irrelevant, 
    #   but if you were to test the code and examine, you would see that it would put the duplicated records next to each other
    #   duplicated() needs keep=False argument to flag all duplicated rows instead of marking last occurrence 
    results_args.update({
        "badrows": results.sort_values(dupcols)[results.duplicated(dupcols, keep=False)].tmp_row.tolist(),
        "badcolumn" : 'BioaccumulationSampleID,SampleDate,AnalysisBatchID,SampleType,Matrix,AnalyteName,LabReplicate,LabSampleID',
        "error_type": "Value Error",
        "error_message" : "These appear to be duplicated records that need to be distinguished with the labreplicate field (labrep 1, and labrep 2)"
    })
    errs.append(checkData(**results_args))



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
        "error_message": "This row is not a 'spike' or a CRM, so the TrueValue should be -88"
    })
    warnings.append(checkData(**results_args))
    
    
    # badrows here could be considered as ones that ARE CRM's / spikes, but the TrueValue is missing (Warning)
    print('# badrows here could be considered as ones that ARE CRMs / spikes, but the TrueValue is missing (Warning)')
    badrows = results[(spike_mask) & ((results.truevalue <= 0) | results.truevalue.isnull())].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is a 'spike' or a CRM, so the TrueValue should not be -88 (or any negative number)"
    })
    warnings.append(checkData(**results_args))

    # Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)
    print("""# Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)""")
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
        "error_message": "The MDL should not be greater than the RL"
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
        "error_message": """If result > RL then the qualifier cannot say 'below reporting limit' or 'below method detection limit'."""
    })
    errs.append(checkData(**results_args))


    # Check - if the qualifier is "less than" or "below method detection limit" Then the result must be -88 (Error)
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
        "error_message": "if Qualifier is 'less than or equal to', 'below method detection limit', 'below reporting limit' or 'estimated', but the Result > RL, then the incorrect qualifier was used"
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
        "error_message": "if the qualifier is 'none' or 'equal to' then the result must be greater than the RL. (This does not apply to Lab blanks, Field blanks or Equipment blanks.)"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.fillna('').comments == '')].tmp_row.tolist(),
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
        "error_message": "We would like to be contacted concerning this record of data. Please contact bight23-im@sccwrp.org"
    })
    warnings.append(checkData(**results_args))


    # ----------------------------------------------------------------------------------------------------------------------------------#
    # Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)
    print('# Check that each analysis batch has all the required sampletypes (All of them should have "Result" for obvious reasons) (Error)')
    # Analyte classes: Inorganics, PAH, PCB, Chlorinated Hydrocarbons, Pyrethroid, PBDE
    # EDIT: From lu_analytes it seems like the Analyte Classes to check are the following: Inorganics, PCB, PCB, Chlorinated Hydrocarbons
    # Required sampletypes by analyteclass:
    # Inorganics: Lab blank, Reference Material, Matrix Spike, Blank Spike
    # PCB: Lab blank, Blank spiked, Result, Reference Material
    # Chlorinated Hydrocarbons: Lab blank, Blank spiked, Matrix spike, Result, Reference - SRM 1974c Mussel Tissue
    # PBDE: Lab blank, Reference Material, Matrix Spike
    # PFAS: Lab blank, Blank spiked, Matrix spike, Result
    # Lipids: does NOT need to be checked for required sampletypes
    error_args = []
    
    required_sampletypes = {
        "Inorganics": ['Lab blank', 'Blank spiked', 'Result','Reference - SRM 2976 Mussel Tissue'],
        # "PCB": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1974c Mussel Tissue'],
        "PCB": ['Lab blank', 'Matrix spike', 'Result', 'Reference - SRM 1974c Mussel Tissue'],
        # "Chlorinated Hydrocarbons": ['Lab blank', 'Blank spiked', 'Matrix spike', 'Result', 'Reference - SRM 1974c Mussel Tissue'],
        "Chlorinated Hydrocarbons": ['Lab blank', 'Matrix spike', 'Result', 'Reference - SRM 1974c Mussel Tissue'],
        "PFAS":['Lab blank', 'Blank spiked', 'Matrix spike', 'Result']
    }


    # anltclass = analyteclass
    # smpltyps = sampletypes
    # Just temp variables for this code block
    for anltclass, smpltyps in required_sampletypes.items():
        print("Check required sampletypes")
        error_args = [*error_args, *chk_required_sampletypes(results, smpltyps, anltclass)]
        
        print("Check for sample duplicates or matrix spike duplicates")
        if anltclass != 'Inorganics':
            print('Non-Inorganics')
            # For Inorganics, they can have either or, so the way we deal with inorganics must be different
            # error_args = [*error_args, *check_dups(results, anltclass, 'Result')]
            error_args = [*error_args, *check_dups(results, anltclass, 'Matrix spike')]
        elif anltclass == 'PFAS':
            error_args = [*error_args, *check_dups(results, anltclass, 'Matrix spike')]
            error_args = [*error_args, *check_dups(results, anltclass, 'Blank spiked')]
        else:
            print('Inorganics')
            error_args = [*error_args, *check_dups(results, anltclass, 'Blank spiked')]
            
    requires_crm = ["Inorganics", "PCB", "Chlorinated Hydrocarbons"]
    error_args = [*error_args, *check_required_crm(results, requires_crm)]
    
    for argset in error_args:
        results_args.update(argset)
        errs.append(checkData(**results_args))

    
    
    # ----------------------------------------------------------------------------------------------------------------------------------#


    # Check - For Inorganics, units must be in ug/g dw (Error)
    print('# Check - For Inorganics, units must be in ug/g dw (Error) (Not CRMs)')
    # --- code --- #


    # Check - For sampletype Lab blank, if Result is less than MDL, it must be -88
    print('# Check - For sampletype Lab blank, if Result is less than MDL, it must be -88')
    # mb_mask = Lab blank mask
    print('# mb_mask = Method (Lab) blank mask')

    # Field and equipment blank only applies to PFAS
    mb_mask = results.sampletype.isin(['Lab blank', 'Field blank', 'Equipment blank'])
    results_args.update({
        "badrows": results[mb_mask & ((results.result < results.mdl) & (results.result != -88))].tmp_row.tolist(),
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "For blank sampletypes (Lab blank, Field blank, Equipment blank), if Result is less than MDL, it must be -88"
    })
    errs.append(checkData(**results_args))

    # Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.
    print('# Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.')
    results_args.update({
        "badrows": results[(mb_mask & (results.result == -88)) & (~results.qualifier.isin(['below method detection limit','none'])) ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "If SampleType=Lab blank, Field blank, or Equipment blank and Result=-88, then qualifier must be 'below method detection limit' or 'none'"
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
    # FIPRONIL has been removed from thsi check (and should be from all checks)
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
    errs.append(checkData(**results_args))

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
    errs.append(checkData(**results_args))
    # ---------------------------------------------------------------------------------------------------------------------------------#
    # ---------------------------------------------------------------------------------------------------------------------------------#
    print('# ---------------------------------------------------------------------------------------------------------------------------------#')
    


    # ------------------------------------------------------------------------------------------------------------#
    print('# ------------------------------------------------------------------------------------------------------------#')
    # Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:
    print('# Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both:')
    # 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"
    print('# 1. "Deltamethrin/Tralomethrin" and "Deltamethrin"')
    # 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"
    print('# 2. "Esfenvalerate/Fenvalerate" and "Esfenvalerate"')
    # 3. "Permethrin, cis" and "Permethrin (cis/trans)"
    print('# 3. "Permethrin, cis" and "Permethrin (cis/trans)"')
    # 4. "Permethrin, trans" and "Permethrin (cis/trans)"
    print('# 4. "Permethrin, trans" and "Permethrin (cis/trans)"')

    results_args.update(pyrethroid_analyte_logic_check(results, ["Deltamethrin/Tralomethrin", "Deltamethrin"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Esfenvalerate/Fenvalerate", "Esfenvalerate"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, cis", "Permethrin (cis/trans)"]))
    errs.append(checkData(**results_args))
    results_args.update(pyrethroid_analyte_logic_check(results, ["Permethrin, trans", "Permethrin (cis/trans)"]))
    errs.append(checkData(**results_args))
    
    # END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......
    print('# END Check - For analyteclass Pyrethroid - within the same analysisbatch, you cant have both .......')
    # ------------------------------------------------------------------------------------------------------------#
    print('# ------------------------------------------------------------------------------------------------------------#')

    # Check - If sampletype is a Reference material, the matrix cannot be "labwater"
    print('# Check - If sampletype is a Reference material, the matrix cannot be "labwater"')
    results_args.update({
        "badrows": results[results.sampletype.str.contains('Reference', case = False) & (results.matrix != 'tissue')].tmp_row.tolist(),
        "badcolumn": "SampleType, Matrix",
        "error_type": "Value Error",
        "error_message": "If sampletype is a Reference material, the matrix must be 'tissue' (for chemistry mussel tissue data submissions)"
    })
    errs.append(checkData(**results_args))
    
    
    # -----------------------------------------------------------------------------------------------------------------------------------#
    # Units checks
    print("# units checks for Mussel tissue")
    organic_tissue_mask = (results.analyteclass != 'Inorganics')
    metals_tissue_mask = (results.analyteclass == 'Inorganics')
    
    # Non reference materials units must be ug/g ww for the tissue matrix (metals)
    print('# Non reference materials units must be ug/g ww for the tissue matrix (metals)')
    results_args.update({
        "badrows": results[ (metals_tissue_mask & (~results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ug/g ww'])) ].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For metals in Mussel Tissue, units must be ug/g ww for non Reference material sampletypes"
    })
    errs.append(checkData(**results_args))
    

    # Non reference materials units must be ng/g ww for the tissue matrix (organics)
    print('# Non reference materials units must be ng/g ww for the tissue matrix (organics)')
    results_args.update({
        "badrows": results[ (organic_tissue_mask & (~results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ng/g ww'])) ].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For organics in Mussel Tissue, units must be ng/g ww for non Reference material sampletypes"
    })
    errs.append(checkData(**results_args))
    

    # May 28, 2023 - Robert
    # Copy pasting this code from the sediment chemistry custom checks file
    # Check - For SampleType/CRM Reference Material and Matrix Sediment, Analyte must have units that match lu_chemcrm. (Error)"
    print("# Check - For SampleType/CRM Reference Material and Matrix Sediment, Analyte must have units that match lu_chemcrm. (Error)")
    crm_analyteclasses = pd.read_sql("SELECT DISTINCT analyteclass FROM lu_chemcrm WHERE matrix = 'tissue'", eng).analyteclass.tolist()
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
        # @Zaib - changed your lookup list link to not have the full hard coded URL, but rather the one that uses the app script root variable
        "error_message": f"For SRM 1974c and SRM 2976, units must match those in the reference material document. <a href=scraper?action=help&layer=lu_chemcrm target=_blank>See the CRM Lookup list values</a>)"
    })
    errs.append(checkData(**results_args))
    

    # May 28, 2023 - Robert
    # Check - sampletype cannot be a sediment reference material
    # Get the sediment CRMs from the lookup list (this way if we ever decide to change their names we dont need to change any code)
    forbidden_crms = pd.read_sql("""SELECT DISTINCT crm FROM lu_chemcrm WHERE matrix = 'sediment';""", eng)
    assert not forbidden_crms.empty, "CRM lookup list lu_chemcrm has no reference materials for the sediment matrix. Check the table in the database, it is likely not configured correctly"
    
    forbidden_crms = forbidden_crms.crm.tolist()
    
    results_args.update({
        "badrows": results[results.sampletype.isin(forbidden_crms)].tmp_row.tolist(),
        "badcolumn": "SampleType",
        "error_type": "Value Error",
        "error_message": f"You are making a tissue chemistry submission but this Reference Material is for sediment"
    })
    errs.append(checkData(**results_args))



    # Check - in a tissue submission, they should be reporting % by weight of Lipids for all samples
    print("# Check - in a tissue submission, they should be reporting % by weight of Lipids for all samples")
    if not results.empty:
        # I cannot think of a single case where results here would be empty, but i always put that
        
        checkdf = results[(results.matrix == 'tissue') & (~results.bioaccumulationsampleid.isin(['LABQC','0000']))]
        if not checkdf.empty:
            checkdf = checkdf.groupby(['bioaccumulationsampleid']).agg({
                    # True if Lipids is in there, False otherwise
                    'analytename' : (lambda grp: 'Lipids' in grp.unique()), 
                    'tmp_row': list
                }) \
                .reset_index() \
                .rename(columns = {'analytename': 'has_lipids'}) # rename to has_lipids since really that is what the column is representing after the groupby operation
            
            bad = checkdf[~checkdf.has_lipids]
            if not bad.empty:
                for _, row in bad.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row,
                        "badcolumn": "BioAccumulationSampleID,AnalyteName",
                        "error_type": "Missing Data",
                        "error_message": f"""For the bioaccumulation sampleid {row.bioaccumulationsampleid} it appears the percent Lipid content was not reported"""
                    })
                    errs.append(checkData(**results_args))
        
        results_args.update({
            "badrows": results[(results.analytename == 'Lipids') & (results.units != f'% by weight')].tmp_row.tolist(),
            "badcolumn": "BioAccumulationSampleID,AnalyteName",
            "error_type": "Missing Data",
            "error_message": f"""If the analytename is 'Lipids' then the units must be '% by weight'"""
        })
        errs.append(checkData(**results_args))
        
    print("# DONE WITH Check - in a tissue submission, they should be reporting % by weight of Lipids for all samples")




    # -----------------------------------------------------------------------------------------------------------------------------------

    # ----- END CUSTOM CHECKS - TISSUE RESULTS ----- #


    # If there are errors, dont waste time with the QA plan checks

    # For testing, let us not enforce this, or we will waste a lot of time cleaning data
    # uncommented to test basic chemistry tissue checks - zaib 2june2023
    # if errs != []:
    #     return {'errors': errs, 'warnings': warnings}



    # -=======- BIGHT CHEMISTRY QA PLAN CHECKS -=======- #  

    results62 = results[results.analyteclass == 'Inorganics']

    if not results62.empty:
        # ------- Table 6-2 - Inorganics (Mercury, Selenium, Arsenic), tissue matrices (tissue and labwater) -------#
        # Table 6-2 Check #1 - Required SampleTypes Already covered above


        # Table 6-2 Check #2 - Result for the CRM sampletype (Reference - SRM 2976 Mussel Tissue) should be within 30% of certified value
        crmvals = pd.read_sql("SELECT crm AS sampletype, analytename, matrix, certified_value FROM lu_chemcrm WHERE crm = 'Reference - SRM 2976 Mussel Tissue';", eng)
        
        tmp = results62.merge(crmvals, on = ['sampletype', 'analytename', 'matrix'], how = 'left')
        
        if not tmp.empty:
            tmp['failed_crmcheck'] = tmp \
                .apply(
                    lambda row: 
                    ((row.result > (row.certified_value * 1.3)) | (row.result < (row.certified_value * 0.7))),
                    axis = 1
                )

            
            results_args.update({
                "badrows": tmp[tmp.failed_crmcheck].tmp_row.tolist(),
                "badcolumn": "result",
                "error_type": "Value Error",
                "error_message": f"""For the reference material 'Reference - SRM 2976 Mussel Tissue' the result should be within 30% of the certified value (See the <a target=_blank href=scraper?action=help&layer=lu_chemcrm>CRM Lookup list</a>)"""
            })
            warnings.append(checkData(**results_args))
        # END Table 6-2 Check #2 - Result for the CRM sampletype (Reference - SRM 2976 Mussel Tissue) should be within 30% of certified value


        # Table 6-2 Check #3 and #6 within an analysisbatch, Blank spikes should have 75-125% recovery of spiked mass for 100% of analytes
        print('# Check #3 - within an analysisbatch, Blank spikes should have 75-125% recovery of spiked mass for all analytes')
        checkdf = results[(results.analyteclass == 'Inorganics') & results.sampletype.isin(['Blank spiked'])] \
            .groupby(['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate']) \
            .apply(
                lambda df: 
                (sum( (df.percentrecovery > 75) & (df.percentrecovery < 125)) / len(df)) == 1 if (len(df) > 0) else True
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Blank spiked'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 75) | (checkdf.percentrecovery > 125))]

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Blank spikes, all analytes should have 75-125% recovery"
            })
            warnings.append(checkData(**results_args))
        # --- END Table 6-2 Check #3 and #6 --- #



        # --- Table 6-2 Check #4 and #7 --- #
        # Matrix spike dupes are required
        # Check #4 - Matrix spike duplicate required (1 per batch) (Not as of 2023-08-08 according to Charles)
        # print('# Check - Matrix spike duplicate required (1 per batch)')
        # tmp = results62.groupby(['analysisbatchid', 'analytename']).apply(
        #     lambda df:
        #     not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        # )
        # if not tmp.empty:
        #     tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
        #     tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
        #     tmp = results62.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
        #     tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
        #     if not tmp.empty:
        #         tmp = tmp.reset_index()
        #         for _, row in tmp.iterrows():
        #             results_args.update({
        #                 "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
        #                 "badcolumn": "SampleType",
        #                 "error_type": "Incomplete data",
        #                 "error_message": f"The batch {row.analysisbatchid} is missing a matrix spike duplicate for {row.analytename}"
        #             })
        #             warnings.append(checkData(**results_args))
        
        #(Check #7, sample as check #4 except with Blank spikes)
        print('# Check #7 - Blank spike duplicate required (1 per batch)')
        print('# Check #7 disabled as of 2023-06-01')
        tmp = results62.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_blankspike_dup') 
            tmp = tmp[~tmp.has_blankspike_dup] # get batches without the matrix spike dupes
            tmp = results62.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a matrix spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        # --- END TABLE 6-2 Check #7 --- #
        # --- END Table 6-2 Check #4 and #7 --- #

        # --- Table 6-2 Check #5 and #8 --- #
        # Blank spike duplicates need RPD under 25%
        print('# Check - Duplicate Blank spikes must have RPD < 25% for all analytes')
        print('# This check disabled for matrix spikes as of 2023-08-08')
        checkdf = results[(results.analyteclass == 'Inorganics') & results.sampletype.isin(['Blank spiked'])]

        if not checkdf.empty:
            checkdf = checkdf.groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'analytename','sampleid']).apply(
                lambda subdf:
                abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.25
            )

            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'rpd_under_25')
                checkdf = checkdf.groupby(['analysisbatchid','analyteclass']).apply(lambda df: sum(df.rpd_under_25) / len(df) == 1 )
                if not checkdf.empty:
                    checkdf = checkdf.reset_index(name = 'passed')
                    checkdf['errmsg'] = checkdf.apply(
                        lambda row:
                        f"Duplicate Blank spikes should have an RPD under 25% for all analytes in the batch ({row.analysisbatchid}) (for the analyteclass {row.analyteclass})"
                        , axis = 1
                    )
                    checkdf = results[(results.analyteclass == 'Inorganics') & results.sampletype.isin(['Blank spiked'])] \
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
        # --- END Table 6-2 Check #5 and #8 --- #
        
        
        # --- Table 6-2 Check #9 --- #
        # For SampleType = Lab blank, we must require Result < MDL or less than 5% of measured concentration in sample
        print('# Check - For Lab blank sampletypes - Result < MDL or Result < 5% of measured concentration in samples (Warning)')
        argslist = MB_ResultLessThanMDL(results[results.analyteclass == 'Inorganics'])
        print("done calling MB ResultLessThanMDL")
        for args in argslist:
            results_args.update(args)
            warnings.append(checkData(**results_args))
        # --- END Table 6-2 Check #9 --- #


    # ------- END Table 6-2 - Inorganics (Mercury, Selenium, Arsenic), tissue matrices (tissue and labwater) -------#


    # ------- Table 6-3 - PCBs, Chlorinated Pesticides, and PFAS, tissue matrices (tissue and labwater) -------#
    mask63 = results.analyteclass != 'Inorganics'
    results63 = results[mask63]

    if not results63.empty:
        # Check #1 - Required SampleTypes Already covered above

        # Table 6-3 Check #2 - Result for the CRM sampletype (Reference - SRM 1974c Mussel Tissue) should be within 50% of certified value (70% of analytes)
        print('# Check - For reference materials - Result should be within 50% of the specified value (in lu_chemcrm) for 70% of the analytes')
        crmvals = pd.read_sql(
            f"""
            SELECT
                lu_chemcrm.analytename,
                lu_chemcrm.matrix,
                lu_chemcrm.certified_value,
                lu_analytes.analyteclass 
            FROM
                lu_chemcrm
                JOIN lu_analytes ON lu_chemcrm.analytename = lu_analytes.analyte 
            WHERE
                crm = 'Reference - SRM 1974c Mussel Tissue'
            """,
            eng
        )
        checkdf = results[mask63 & results.sampletype.str.contains('Reference', case = False)]
        if not checkdf.empty:
            checkdf = checkdf.merge(crmvals, on = 'analytename', how = 'left')
        
        if not checkdf.empty:
            checkdf['within40pct'] = checkdf.apply(
                    lambda row:
                    (0.6 * float(row.certified_value)) <= row.result <= (1.4 * float(row.certified_value)) if not pd.isnull(row.certified_value) else True
                    ,axis = 1
                )
            checkdf = checkdf.merge(
                checkdf.groupby('analysisbatchid') \
                    .apply(
                        lambda df: sum(df.within40pct) / len(df) < 0.7
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
                "error_message": "Less than 70% of the analytes in this batch are within 50% of the CRM value"
            })
            warnings.append(checkData(**results_args))
        # END Table 6-3 Check #2 - Result for the CRM sampletype (Reference - SRM 1974c Mussel Tissue) should be within 50% of certified value (70% of analytes)



        # Table 6-3 Check #3 - % recovery must be 50 to 150% for at least 70% of analytes
        print('# Check - within an analysisbatch, Matrix spikes/Blank spikes should have 50-150% recovery of spiked mass for at least 70% of analytes')
        checkdf = results[(results.analyteclass != 'Inorganics') & results.sampletype.isin(['Matrix spike'])] \
            .groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'sampleid', 'labreplicate']) \
            .apply(
                lambda df: 
                (sum( (df.percentrecovery > 50) & (df.percentrecovery < 150)) / len(df)) > 0.7 if (len(df) > 0) else True
            )
        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'passed_check')
            checkdf = results.merge(checkdf, on = ['analysisbatchid', 'analyteclass', 'sampletype', 'sampleid', 'labreplicate'], how = 'inner')
            checkdf = checkdf[checkdf.sampletype.isin(['Matrix spike'])]
            checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 50) | (checkdf.percentrecovery > 150))]

            results_args.update({
                "badrows": checkdf.tmp_row.tolist(),
                "badcolumn": "AnalysisBatchID, AnalyteName, SampleType, LabSampleID, LabReplicate, Result",
                "error_type": "Value Error",
                "error_message": f"For Matrix spikes (for Organics in Tissue), 70% or more of the analytes within each analyteclass should have 50-150% recovery"
            })
            warnings.append(checkData(**results_args))
        # END Table 6-3 Check #3 - % recovery must be 50 to 150% for at least 70% of analytes

        # Table 6-3 Check #4 Matrix spike duplicate required
        print('# Check - Matrix spike duplicate required (1 per batch)')

        tmp = results63.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Matrix spike') & (df.labreplicate == 2)].empty # signifies whether or not a Matrix spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_matrixspike_dup') 
            tmp = tmp[~tmp.has_matrixspike_dup] # get batches without the matrix spike dupes
            tmp = results63.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
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
        # END Table 6-3 Check #4 Matrix spike duplicate required

        # Table 6-3 Check #5 Matrix spike duplicate RPD < 50% for at least 70% of analytes
        print('# Check - Duplicate Matrix spikes must have RPD < 50% for all analytes')
        checkdf = results63[results63.sampletype.isin(['Matrix spike'])]
        checkdf = checkdf.groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.5
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_50')
            checkdf = checkdf.groupby(['analysisbatchid','analyteclass']).apply(lambda df: sum(df.rpd_under_50) / len(df) >= 0.7 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Matrix spikes should have an RPD under 50% for at least 70% analytes in the batch ({row.analysisbatchid}) (for the analyteclass {row.analyteclass})"
                    , axis = 1
                )
                checkdf = results63[results63.sampletype.isin(['Matrix spike'])] \
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

        # END Table 6-3 Check #5 Matrix spike duplicate RPD < 50% for at least 70% of analytes



        # ----- PFAS Only ----- #
        results63pfas = results63[results63.analyteclass == 'PFAS']
        # Table 6-3 Check #6 - Blank spiked % recovery must be 50 to 150% (PFAS ONLY)
        print('# Check - within an analysisbatch, Blank spikes should have 50-150% recovery of spiked mass for all PFAS analytes')
        checkdf = results63pfas[results63pfas.sampletype.isin(['Blank spiked'])] 

        if not checkdf.empty:
            checkdf = checkdf \
                .groupby(['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate']) \
                .apply(
                    lambda df: 
                    (sum( (df.percentrecovery > 50) & (df.percentrecovery < 150)) / len(df)) == 1 if (len(df) > 0) else True
                )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed_check')
                checkdf = results.merge(checkdf, on = ['analysisbatchid', 'sampletype', 'sampleid', 'labreplicate'], how = 'inner')
                checkdf = checkdf[checkdf.sampletype.isin(['Blank spiked'])]
                checkdf = checkdf[(~checkdf.passed_check) & ((checkdf.percentrecovery < 50) | (checkdf.percentrecovery > 150))]

                results_args.update({
                    "badrows": checkdf.tmp_row.tolist(),
                    "badcolumn": "AnalysisBatchID, SampleType, LabSampleID, LabReplicate, Result",
                    "error_type": "Value Error",
                    "error_message": f"For Blank spikes, all analytes should have 50-150% recovery (for PFAS)"
                })
                warnings.append(checkData(**results_args))
        # END Table 6-3 Check #6 - Blank spiked % recovery must be 50 to 150% (PFAS ONLY)


        # Table 6-3 Check #7 Blank spike duplicate required (PFAS ONLY)
        print('# Check - Blank spike duplicate required (1 per batch)')

        tmp = results63pfas.groupby(['analysisbatchid', 'analytename']).apply(
            lambda df:
            not df[(df.sampletype == 'Blank spiked') & (df.labreplicate == 2)].empty # signifies whether or not a blank spike duplicate is present
        )
        if not tmp.empty:
            tmp = tmp.reset_index( name = 'has_blankspike_dup') 
            tmp = tmp[~tmp.has_blankspike_dup] # get batches without the blank spike dupes
            tmp = results63pfas.merge(tmp, on = ['analysisbatchid', 'analytename'], how = 'inner')
            tmp = tmp.groupby(['analysisbatchid', 'analytename']).agg({'tmp_row': list})
            if not tmp.empty:
                tmp = tmp.reset_index()
                for _, row in tmp.iterrows():
                    results_args.update({
                        "badrows": row.tmp_row, # list of rows associated with the batch that doesnt have a blank spike dup
                        "badcolumn": "SampleType",
                        "error_type": "Incomplete data",
                        "error_message": f"The batch {row.analysisbatchid} is missing a blank spike duplicate for {row.analytename}"
                    })
                    warnings.append(checkData(**results_args))
        # END Table 6-3 Check #7 Blank spike duplicate required (PFAS ONLY)



        # Table 6-3 Check #8 Blank spike duplicate RPD < 30% for all analytes (PFAS ONLY)
        print('# Check - Duplicate Matrix spikes must have RPD < 30% for all analytes')
        checkdf = results63pfas[results63pfas.sampletype.isin(['Blank spiked'])]
        checkdf = checkdf.groupby(['analysisbatchid', 'analyteclass', 'sampletype', 'analytename','sampleid']).apply(
            lambda subdf:
            abs((subdf.result.max() - subdf.result.min()) / ((subdf.result.max() + subdf.result.min()) / 2)) <= 0.3
        )

        if not checkdf.empty:
            checkdf = checkdf.reset_index(name = 'rpd_under_30')
            checkdf = checkdf.groupby(['analysisbatchid','analyteclass']).apply(lambda df: sum(df.rpd_under_30) / len(df) == 1 )
            if not checkdf.empty:
                checkdf = checkdf.reset_index(name = 'passed')
                checkdf['errmsg'] = checkdf.apply(
                    lambda row:
                    f"Duplicate Blank spikes should have an RPD under 30% for all analytes in the batch ({row.analysisbatchid}) (for the analyteclass {row.analyteclass})"
                    , axis = 1
                )
                checkdf = results63pfas[results63pfas.sampletype.isin(['Blank spiked'])] \
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
        # END Table 6-3 Check #8 Blank spike duplicate RPD < 30% for all analytes (PFAS ONLY)
        # ----- END PFAS Only ----- #


        # Table 6-3 Check #9 Result must be less than 10*MDL for lab blanks
        results_args.update({
            "badrows": results[ (results.sampletype == 'Lab blank') & (results.result >= (results.mdl * 10))].tmp_row.tolist(),
            "badcolumn": "Result",
            "error_type": "Value Warning",
            "error_message": "For Lab blanks, the result must be less than 10 times the MDL"
        })
        warnings.append(checkData(**results_args))


        # Table 6-3 Check #9 Result must be less than 10*MDL AND less than the RL for Field/Equipment blanks
        results_args.update({
            "badrows": results[ 
                
                (results.sampletype.isin(['Field blank', 'Equipment blank']) ) 
                & ( (results.result >= (results.mdl * 10)) | (results.result > results.rl) )

            ].tmp_row.tolist(),
            
            "badcolumn": "Result",
            "error_type": "Value Warning",
            "error_message": "For Field and Equipment blanks, the result must be less than 10 times the MDL, and be less than the RL"
        })
        warnings.append(checkData(**results_args))
        
        # END Table 6-3 Check #9 Result must be less than 10*MDL


    # ------- END Table 6-3 - PCBs, Chlorinated Pesticides, and PFAS, tissue matrices (tissue and labwater) -------#
    
    return {'errors': errs, 'warnings': warnings}
