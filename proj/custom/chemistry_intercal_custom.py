# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from datetime import timedelta
from .functions import checkData, checkLogic, sample_assignment_check
from .chem_functions_custom import *
import pandas as pd
import re

def chemistry_intercal(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    if not expectedtables.issubset(set(all_dfs.keys())):
        print(f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})""")

    # DB Connection
    eng = g.eng

    # define errors and warnings list
    errs = []
    warnings = []

    results = all_dfs['tbl_chemresults_intercal']

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
            float(x.result)/float(x.truevalue)*100 
            if (('spike' in x.sampletype.lower())|('reference' in x.sampletype.lower())) & (x.truevalue != 0)
            else -88, 
            axis = 1
        )


    results_args = {
        "dataframe": results,
        "tablename": 'tbl_chemresults_intercal',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    
    # ----- CUSTOM CHECKS  ----- #
    print('# ----- CUSTOM CHECKS ----- #')

    
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
    badrows = results[(spike_mask) & (results.truevalue < 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "TrueValue",
        "error_type": "Value Error",
        "error_message": "This row is a 'spike' or a CRM, so the TrueValue should not be -88 (or any negative number)"
    })
    warnings.append(checkData(**results_args))

    # Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)
    print("""# Check - Result column should be a positive number (except -88) for SampleType == 'Result' (Error)""")
    badrows = results[(results.sampletype == 'Result') & (results.result.apply(lambda x: float(x)) != -88) & (results.result.apply(lambda x: float(x)) <= 0)].tmp_row.tolist()
    results_args.update({
        "badrows": badrows,
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "The Result column (for SampleType = 'Result') should be a positive number (unless it is -88)"
    })
    errs.append(checkData(**results_args))

    # Check - The MDL should never be greater than the RL (Error)
    print('# Check - The MDL should never be greater than the RL (Error)')
    results_args.update({
        "badrows": results[results.mdl.apply(lambda x: float(x)) > results.rl.apply(lambda x: float(x))].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should never be greater than the RL"
    })
    warnings.append(checkData(**results_args))
    
    # Check - The MDL should not be equal to the RL (Warning)
    print('# Check - The MDL should not be equal to the RL (Warning)')
    results_args.update({
        "badrows": results[results.mdl.apply(lambda x: float(x)) == results.rl.apply(lambda x: float(x))].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be equal the RL in most cases"
    })
    warnings.append(checkData(**results_args))
    
    # Check - The MDL should never be a negative number (Error)
    print('# Check - The MDL should never be a negative number (Error)')
    results_args.update({
        "badrows": results[(results.mdl.apply(lambda x: float(x)) < 0) & (results.mdl.apply(lambda x: float(x)) != -88)].tmp_row.tolist(),
        "badcolumn": "MDL",
        "error_type": "Value Error",
        "error_message": "The MDL should not be negative (except for -88 to denote a missing value)"
    })
    errs.append(checkData(**results_args))


    
    # Check - if the qualifier is "less than" or "below method detection limit" Then the result must be -88 (Error)
    print('# Check - if the qualifier is "less than" or "below method detection limit" Then the result must be -88 (Error)')
    results_args.update({
        "badrows": results[results.qualifier.isin(["less than", "below method detection limit"]) & (results.result.astype(float) != -88)].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'less than' or 'below method detection limit' then the Result should be -88"
    })
    warnings.append(checkData(**results_args))

    # Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)
    print('# Check - if the qualifier is "estimated" or "below reporting level" then the result must be between the mdl and rl (inclusive) (Error)')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["estimated", "below reporting level"])) & ((results.result.apply(lambda x: float(x)) < results.mdl.apply(lambda x: float(x))) | (results.result.apply(lambda x: float(x)) > results.rl.apply(lambda x: float(x))))
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "If the Qualifier is 'estimated' or 'below reporting level' then the Result should be between the MDL and RL (inclusive)"
    })
    warnings.append(checkData(**results_args))
    
    # Check - if the qualifier is less than, below mdl, below reporting level, or estimated, but the result > rl, then the wrong qualifier was used
    print('# Check - if the qualifier is less than, below mdl, below reporting level, or estimated, but the result > rl, then the wrong qualifier was used')
    results_args.update({
        "badrows": results[
                (results.qualifier.isin(["estimated", "below reporting level", "below method detection limit", "estimated"])) 
                & (results.result.apply(lambda x: float(x)) > results.rl.apply(lambda x: float(x)))
            ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "if qualifier is 'less than', 'below method detection limit', 'below reporting level' or 'estimated', but the Result > RL, then the incorrect qualifier was used"
    })
    errs.append(checkData(**results_args))

    # Check - if the qualifier is "none" then the result must be greater than the RL (Error)
    print('# Check - if the qualifier is "none" then the result must be greater than the RL (Error)')
    results_args.update({
        "badrows": results[(results.qualifier == 'none') & (results.result.apply(lambda x: float(x)) < results.rl.apply(lambda x: float(x)))].tmp_row.tolist(),
        "badcolumn": "Qualifier, Result",
        "error_type": "Value Error",
        "error_message": "if the qualifier is 'none' then the result must be greater than or equal to the RL (See the <a href=/bight23checker/scraper?action=help&layer=lu_chemqualifiercodes target=_blank>qualifier lookup list</a> for reference)"
    })
    errs.append(checkData(**results_args))

    # Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)
    print('# Check - Comment is required if the qualifier says "analyst error" "contaminated" or "interference" (Error)')
    results_args.update({
        "badrows": results[(results.qualifier.isin(["analyst error","contaminated","interference"])) & (results.fillna('').comments == '')].tmp_row.tolist(),
        "badcolumn": "Comments",
        "error_type": "Value Error",
        "error_message": "We would like you to enter a comment if the qualifier says 'analyst error' 'contaminated' or 'interference'"
    })
    warnings.append(checkData(**results_args))
    
    # Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)
    print('# Check - We would like the submitter to contact us if the qualifier says "analyst error" (Warning)')
    results_args.update({
        "badrows": results[results.qualifier == "analyst error"].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "We would like to be contacted concerning this record of data. Please contact bight23-im@sccwrp.org"
    })
    warnings.append(checkData(**results_args))

    # Check - True Value should not be Zero
    print('# Check - True Value should not be Zero')
    results_args.update({
        "badrows": results[results.truevalue == 0].tmp_row.tolist(),
        "badcolumn": "truevalue",
        "error_type": "Value Error",
        "error_message": "The TrueValue should never be zero. If the TrueValue is unknown, then please fill in the cell with -88"
    })
    errs.append(checkData(**results_args))

    # Check - For sampletype Lab blank, if Result is less than MDL, it must be -88
    print('# Check - For sampletype Lab blank, if Result is less than MDL, it must be -88')
    # mb_mask = Lab blank mask
    print('# mb_mask = Method (Lab) blank mask')
    mb_mask = (results.sampletype == 'Lab blank') 
    results_args.update({
        "badrows": results[mb_mask & ((results.result.apply(lambda x: float(x)) < results.mdl.apply(lambda x: float(x))) & (results.result.apply(lambda x: float(x)) != -88))].tmp_row.tolist(),
        "badcolumn": "Result",
        "error_type": "Value Error",
        "error_message": "For Lab blank sampletypes, if Result is less than MDL, it must be -88"
    })
    errs.append(checkData(**results_args))

    # Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.
    print('# Check - If SampleType=Lab blank and Result=-88, then qualifier must be below MDL or none.')
    results_args.update({
        "badrows": results[(mb_mask & (results.result.apply(lambda x: float(x)) == -88)) & (~results.qualifier.isin(['below method detection limit','none'])) ].tmp_row.tolist(),
        "badcolumn": "Qualifier",
        "error_type": "Value Error",
        "error_message": "If SampleType=Lab blank and Result=-88, then qualifier must be 'below method detection limit' or 'none'"
    })
    errs.append(checkData(**results_args))

    # Units checks on CRMs
    sed_mask = ((results.matrix == 'sediment') & (results.analyteclass.isin(['Chlorinated Hydrocarbons', 'PBDE', 'PCB'])))
    unit_crm_mask = (sed_mask & (results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ng/g dw', 'ug/kg dw']))
    pah_sed_mask = ((results.matrix == 'sediment') & (results.analyteclass == 'PAH'))
    pah_unit_crm_mask = (pah_sed_mask & (results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ug/g dw', 'mg/kg dw']))
    organic_tissue_mask = ((results.matrix == 'tissue') & (results.analyteclass != 'Inorganics'))
    metals_tissue_mask = ((results.matrix == 'tissue') & (results.analyteclass == 'Inorganics'))

    results_args.update({
        "badrows": results[unit_crm_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for Chlorinated Hydrocarbons, PBDE, PCB (Reference Material sampletypes), the units must be in ng/g dw or ug/kg dw"
    })
    errs.append(checkData(**results_args))

    results_args.update({
        "badrows": results[pah_unit_crm_mask].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"for PAH's, and Reference Material sampletypes, the units must be in ug/g dw or mg/kg dw"
    })
    errs.append(checkData(**results_args))

    # If it is a CRM, then units must be ug/kg ww (organics)
    results_args.update({
        "badrows": results[ (organic_tissue_mask & (results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ug/kg ww'])) ].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For Reference materials (for organics) in Mussel Tissue, units must be ug/kg ww"
    })
    errs.append(checkData(**results_args))
        
    # If it is a CRM, then units must be mg/kg dw or ug/g dw (metals)
    results_args.update({
        "badrows": results[ (metals_tissue_mask & (results.sampletype.str.contains('Reference', case = False))) & (~results.units.isin(['ug/g dw', 'mg/kg dw'])) ].tmp_row.tolist(),
        "badcolumn": "Units",
        "error_type": "Value Error",
        "error_message": f"For Reference materials (for metals) in Mussel Tissue, units must be mg/kg dw or ug/g dw"
    })
    errs.append(checkData(**results_args))


    print('# Check - If sampletype is a Reference material, the matrix cannot be "labwater" - it must be sediment')
    results_args.update({
        "badrows": results[results.sampletype.str.contains('Reference', case = False) & (~results.matrix.isin(['sediment', 'tissue']))].tmp_row.tolist(),
        "badcolumn": "SampleType, Matrix",
        "error_type": "Value Error",
        "error_message": f"If sampletype is a Reference material, the matrix cannot be 'labwater' - Rather, it must be sediment or tissue"
    })
    warnings.append(checkData(**results_args))

    # ---------- check - if the matrix is Ottawa sand, the sampletype must be Lab blank ------------- #
    results_args.update({
        "badrows": results[(results.matrix == 'Ottawa sand') & (~results.sampletype.isin(['Lab blank','Blank spiked']) )].tmp_row.tolist(),
        "badcolumn": "sampletype",
        "error_type": "Value Error",
        "error_message": f"if the matrix is Ottawa sand, the sampletype must be Lab blank or Blank spiked"
    })
    warnings.append(checkData(**results_args))




    return {'errors': errs, 'warnings': warnings}
