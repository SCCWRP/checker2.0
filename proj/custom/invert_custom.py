# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, multivalue_lookup_check, mismatch
from sqlalchemy import create_engine
import pandas as pd
import re


def invert(all_dfs):

    current_function_name = str(currentframe().f_code.co_name)

    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(
        current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    trawlinvertebrateabundance = all_dfs['tbl_trawlinvertebrateabundance']
    trawlinvertebratebiomass = all_dfs['tbl_trawlinvertebratebiomass']

    trawlinvertebrateabundance = trawlinvertebrateabundance.assign(tmp_row = trawlinvertebrateabundance.index)
    trawlinvertebratebiomass = trawlinvertebratebiomass.assign(tmp_row = trawlinvertebratebiomass.index)

    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # STARTING CHECKS

    # initialize the connection
    eng = g.eng

    ## LOGIC ##
    # 1a. Logic check - each record in the trawlinvertebrate abundance and biomass has to have a corresponding record in the tbl_trawlevent #
    print("Invert Custom Checks")
    print("each record in the trawlinvertebrate abundance and biomass has to have a corresponding record in the tbl_trawlevent")
    matchcols = ['stationid','sampledate','samplingorganization','trawlnumber']
    trawlevent = pd.read_sql("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;", eng)
    
    trawlinvertebrateabundance_args.update({
        "badrows": mismatch(trawlinvertebrateabundance, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlinvertebrateabundance must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]
    
    # 1b
    trawlinvertebratebiomass_args.update({
        "badrows": mismatch(trawlinvertebratebiomass, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlinvertebratebiomass must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]

    # ----------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 2. Check abundance and biomass dataframes against the field assignment table
    print("Return error if abundance records are not found in field assignment table")

    # Get the FAT records
    fat = pd.read_sql("""SELECT stationid,assigned_agency AS trawlagency FROM field_assignment_table WHERE "parameter" = 'trawl';""", eng)
    unique_fat_records = [] if fat.empty else fat.apply(lambda row: (row.stationid, row.trawlagency), axis = 1).tolist()
    
    # 2a
    print("Invert Custom Checks")
    print("compare biomass records to field assignment table records (compare on stationid,samplingorganization).")
    # Logic check - compare biomass records to field assignment table records (compare on stationid,samplingorganization).
    # same check exists for abundance
    badrows = trawlinvertebrateabundance[
        trawlinvertebrateabundance[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].tmp_row.tolist()
    trawlinvertebrateabundance_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]

    # 2b
    print("Invert Custom Checks")
    print("compare biomass records to field assignment table records (compare on stationid,samplingorganization).")
    # Logic check - compare biomass records to field assignment table records (compare on stationid,samplingorganization).
    # same check exists for abundance
    badrows = trawlinvertebratebiomass[
        trawlinvertebratebiomass[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].tmp_row.tolist()
    trawlinvertebratebiomass_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 3a
    print("Invert Custom Checks")
    print("Logic checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records.")
    # Logic checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records. 
    #    "Composite weight" should be only mismatch.  Error message - Orphan records for biomass vs abundance.
    # 
    matchcols = ['stationid','sampledate','samplingorganization','invertspecies']
    trawlinvertebratebiomass_args.update({
        "badrows": mismatch(
            trawlinvertebratebiomass[~trawlinvertebratebiomass.invertspecies.str.lower().isin(['composite weight'])], 
            trawlinvertebrateabundance[~trawlinvertebrateabundance.invertspecies.str.lower().isin(['composite weight'])], 
            matchcols
        ),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in biomass must match a record in abundance - records are matched based on {','.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]
    
    # 3b
    # Check for those in abundance but not in biomass
    trawlinvertebrateabundance_args.update({
        "badrows": mismatch(
            trawlinvertebrateabundance[~trawlinvertebrateabundance.invertspecies.str.lower().isin(['composite weight'])], 
            trawlinvertebratebiomass[~trawlinvertebratebiomass.invertspecies.str.lower().isin(['composite weight'])], 
            matchcols
        ),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in abundance must match a record in biomass - records are matched based on {','.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    

    ## END LOGIC CHECKS ##
    print("## END LOGIC CHECKS ##")

    ## CUSTOM CHECKS ##
    ############################
    # ABUNDANCE/BIOMASS CHECKS #
    ############################
    print("## CUSTOM CHECKS ##")

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 4a
    # Jordan - Anomaly - If Anomaly = Other, a comment is required.
    # print('If Anomaly = Other, a comment is required.')
    missing_comments = trawlinvertebrateabundance[
        (trawlinvertebrateabundance.anomaly == 'Other') & 
        ((trawlinvertebrateabundance.comments == '') | (trawlinvertebrateabundance.comments.isnull()))
    ]
    print(missing_comments)
    badrows = missing_comments.tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "anomaly,comments",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            'A comment is required for all anomalies listed as Other.'
    }
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]
    
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #



    # 5a
    # Duy: The below function replaces dcValueAgainstMultipleValues
    # Jordan - Anomaly Check - A single anomaly is required but multiple anomalies are possible (many to many).
    tmpargs = multivalue_lookup_check(
        trawlinvertebrateabundance,
        'anomaly',
        'lu_invertanomalies',
        'anomaly',
        eng,
        displayfieldname="Anomaly"
    )
    trawlinvertebrateabundance_args.update(tmpargs)
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 6a
    # Jordan - Anomaly - Check for single records that contain anomalies.
    print('Anomaly - Check for single records that contain anomalies.')
    single_records = trawlinvertebrateabundance[
        ~trawlinvertebrateabundance.duplicated(
            subset = ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber','invertspecies'],
            keep=False
        )
    ]
    badrows = single_records[single_records.anomaly.str.lower() != 'none'].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "anomaly",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            'Anomalies and clean organisms may be lumped together( e.g. 128 urchins, all with parasites isnt likely'
    }
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    
    # NOTE - The code that checks agains SCAMIT 12 is bight 18 code
    #   needs to be updated
    #   Bight 23 is going to use SCAMIT 14
    #   We still need the updated list

    # 7a
    # Jordan - Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name
    print("Species - Check Southern California Association of Marine Invertebrate Taxonomists Edition 12 - Check old species name")
    spcs_names = eng.execute("SELECT synonym, taxon FROM lu_invertsynonyms;")
    sn = pd.DataFrame(spcs_names.fetchall()); sn.columns = spcs_names.keys()

    badrows = trawlinvertebrateabundance[
        trawlinvertebrateabundance.invertspecies.isin(sn.synonym.tolist())
    ].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            f'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]

    # 4b
    badrows = trawlinvertebratebiomass[
        trawlinvertebratebiomass.invertspecies.isin(sn.synonym.tolist())
    ].tmp_row.tolist()
    trawlinvertebratebiomass_args = {
        "dataframe": trawlinvertebratebiomass,
        "tablename": 'tbl_trawlinvertebratebiomass',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Warning",
        "is_core_error": False,
        "error_message":
            f'The species you entered is possibly a synonym. Please verify by checking the lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertsynonyms target=_blank>lu_invertsynonyms</a>'
    }
    warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 8a
    # Jordan - Range check - Check depth ranges (min & max) for all species.
    print("Range check - Check depth ranges (min & max) for all species.")
    # 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent
    trawl_depths = pd.read_sql("SELECT stationid,sampledate,samplingorganization,trawlnumber,startdepth,enddepth FROM tbl_trawlevent;", eng)

    # 2nd. Get Min and Max Depths for each Species from lu_invertspeciesdepthrange
    depth_ranges = pd.read_sql("SELECT species AS invertspecies,mindepth,maxdepth FROM lu_invertspeciesdepthrange;", eng)

    # 3rd. Merge Trawl Depth Records and Submitted Invertebrate Abundance Records on StationID/SampleDate/SamplingOrganization
    tam = trawlinvertebrateabundance[
        ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'invertspecies', 'tmp_row']
    ]\
        .merge(
            depth_ranges, 
            on=['invertspecies']
        ).merge(
            trawl_depths, 
            on=['stationid', 'sampledate', 'samplingorganization', 'trawlnumber']
        )

    if not tam.empty:
        tam['inrange'] = tam.apply(
            lambda x: 
            False if (max(x.startdepth, x.enddepth) < x.mindepth) | (min(x.startdepth, x.enddepth) > x.maxdepth) else True, axis=1
        )
        badrecords = tam[tam.inrange == False]
        
        for i, row in badrecords.iterrows():
            
            trawlinvertebrateabundance_args = {
                "dataframe": trawlinvertebrateabundance,
                "tablename": 'tbl_trawlinvertebrateabundance',
                "badrows": [row.tmp_row],
                "badcolumn": "invertspecies",
                "error_type": "Undefined Warning",
                "is_core_error": False,
                "error_message":
                    '%s was caught in a depth range (%sm - %sm) that does not include the range it is typically found (%sm - %sm). Please verify the species is correct. Check <a href=/%s/scraper?action=help&layer=lu_invertspeciesdepthrange target=_blank>lu_invertspeciesdepthrange</a> for more information.' % (tam.invertspecies[i], int(tam.startdepth[i]), int(tam.enddepth[i]), tam.mindepth[i], tam.maxdepth[i], current_app.script_root)
            }
            warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]
        print("done with for loop")

    # We only run the depth check on abundance since all species in abundance are also in biomass, based on the logic check near the top
    

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 9a
    # Warn if the species is in lu_invertspeciesnotallowed
    # Jordan - Species - Check list of non-trawl taxa (next tab)
    invalid_species = eng.execute("SELECT species AS invertspecies FROM lu_invertspeciesnotallowed;")
    invs = pd.DataFrame(invalid_species.fetchall()); invs.columns= invalid_species.keys()
    badrows = trawlinvertebrateabundance[trawlinvertebrateabundance.invertspecies.isin(invs.invertspecies.tolist())].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Undefined Error",
        "is_core_error": False,
        "error_message":
            f'Holoplanktonic or infaunal species. See lookup list: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_invertspeciesnotallowed target=_blank>lu_invertspeciesnotallowed</a>'
    }
    warnings = [*warnings, checkData(**trawlinvertebrateabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 10a
    # Error if Composite weight is found in abundance tab
    
    badrows = trawlinvertebrateabundance[trawlinvertebrateabundance.invertspecies.str.lower() == 'composite weight'].tmp_row.tolist()
    trawlinvertebrateabundance_args = {
        "dataframe": trawlinvertebrateabundance,
        "tablename": 'tbl_trawlinvertebrateabundance',
        "badrows": badrows,
        "badcolumn": "invertspecies",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Composite weight cannot be a species in the abundance tab"
    }
    errs = [*errs, checkData(**trawlinvertebrateabundance_args)]



    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #



    #######################
    # BIOMASS ONLY CHECKS #
    #######################




    print("# BIOMASS ONLY CHECKS #")
    print("Fish Custom Checks")
    print("If it was submitted as 0 it should rather be submitted as <0.01kg")
    # If it was submitted as 0 it should rather be submitted as <0.01kg
    trawlinvertebratebiomass_args.update({
        "badrows": trawlinvertebratebiomass[ (trawlinvertebratebiomass.biomass < 0.01) & (trawlinvertebratebiomass.biomass != -88) ].tmp_row.tolist(),
        "badcolumn": "Biomass",
        "error_type": "Value Error",
        "error_message": 'Any weight less than 0.01kg should be submitted as <0.01kg (0.01 in the biomass column, "<" in the biomassqualifier column)'
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]


    print("Kristin - If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.01")
    #Rounding biomass to the nearest 0.01
    trawlinvertebratebiomass['biomass'] = [round(trawlinvertebratebiomass['biomass'][x], 2) for x in trawlinvertebratebiomass.index.tolist()]
    print(trawlinvertebratebiomass[(trawlinvertebratebiomass['biomass'] < .01)&~(trawlinvertebratebiomass['biomassqualifier'].isin(['<']))])
    
    badrows = trawlinvertebratebiomass[
        (trawlinvertebratebiomass['biomass'] < .01) &
        (trawlinvertebratebiomass['biomass'] != -88)
    ].tmp_row.tolist()
    
    trawlinvertebratebiomass_args.update({
        "badrows": badrows,
        "badcolumn": "biomass",
        "error_type": "Undefined Error",
        "error_message": 'Biomass values that were less than 0.01 kg (e.g. 0.004 kg) should have been submitted as <0.01 kg (0.01 in biomass column, < in the biomassqualifier column)'
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]  

    #Jordan - Biomass - Filter qualifiers to make sure that all < have corresponding values of 0.01
    print('Biomass - Filter qualifiers to make sure that all < have corresponding values of 0.01')
    badrows = trawlinvertebratebiomass[
        (trawlinvertebratebiomass.biomassqualifier == '<') & 
        (~trawlinvertebratebiomass.biomass.isin([0.01, 0.1]))
    ].tmp_row.tolist()
    trawlinvertebratebiomass_args.update({
        "badrows": badrows,
        "badcolumn": "biomass",
        "error_type": "Undefined Error",
        "error_message": 'Less than qualifiers (<) must have corresponding biomass value of 0.01kg or 0.1kg.'
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)] 




    # Jordan said the below is no longer necessary, but that only applies to the year 2018
    # Need to make sure if it's necessary for 2023, so I am going to leave the below code commented out in case we need it - Duy
    
    # NOTE: The following 4 checks are no longer necessary because composite weights will not be submitted. -Jordan 9/12/2018
    # NOTE: The following 4 checks may now be necessary because composite weights will be submitted. -Robert 7/13/2023
    

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #
    # Basically, this is saying if there are any "<0.01 kg" records, then there must be a composite weight record in the biomass tab
    # Dario says we will retain this check, but we will make it a warning rather than an error (7/19/2023)

    # Jordan - Biomass - Check to make sure that all "<0.01 kg" records have corresponding "Composite Weight" totals.
    less_than_records = trawlinvertebratebiomass[(trawlinvertebratebiomass.biomassqualifier == '<') & (trawlinvertebratebiomass.biomass == 0.01) & (trawlinvertebratebiomass.invertspecies.str.lower() != 'composite weight') ]
    composite_weight_records = trawlinvertebratebiomass[ (trawlinvertebratebiomass.invertspecies.str.lower() == 'composite weight') ]
    badrows = trawlinvertebratebiomass[
            # its a bad row if the stationid is in the set of stations that have "<" qualifiers, but no composite weight records
            (
                trawlinvertebratebiomass.stationid.isin( list(set(less_than_records.stationid.tolist()) - set(composite_weight_records.stationid.tolist())) )
            ) & (
                trawlinvertebratebiomass.biomassqualifier == '<'
            )
        ] \
        .tmp_row.tolist()
    
    trawlinvertebratebiomass_args.update({
        "badrows": badrows,
        "badcolumn": "stationid,invertspecies,biomass,biomassqualifier",
        "error_type": "Undefined Error",
        "error_message": 'This station has records with a "<" qualifier, so there should also be a composite weight record for the station as well'
    })
    warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)] 

    # errorLog('Biomass - Check to make sure that all <0.01 kg records have corresponding Composite Weight totals.')
    # checkData(biomass[biomass.stationid.isin(set(lt.stationid)-set(cp.stationid))].tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','<0.01 kg records submitted but not accompanying composite weight record.',biomass)
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #
    

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #
    
    # Dario says we will retain this check, but we will make it a warning rather than an error (7/19/2023)

    # Jordan - Biomass - Check to see that each Composite weight has a corresponding record of "<0.01 kg".
    badrows = trawlinvertebratebiomass[
            # its a bad row if the stationid is in the set of stations that have composite weight records, but no "<" qualifiers
            (
                trawlinvertebratebiomass.stationid.isin( list(set(composite_weight_records.stationid.tolist()) - set(less_than_records.stationid.tolist())) )
            ) & (
                trawlinvertebratebiomass.invertspecies.str.lower() == 'composite weight'
            )
        ] \
        .tmp_row.tolist()
    
    trawlinvertebratebiomass_args.update({
        "badrows": badrows,
        "badcolumn": "stationid,invertspecies,biomass,biomassqualifier",
        "error_type": "Undefined Error",
        "error_message": 'This station has composite weight records, so there should also be at least one record with a biomass qualifier of "<" for the station as well'
    })
    warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)] 
    
    # errorLog('Check to see that each Composite weight has a corresponding record of <0.01 kg.')
    # checkData(biomass[biomass.stationid.isin(set(cp.stationid)-set(lt.stationid))].tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','Composite weight submitted, but no accompanying <0.01 kg records for that station.',biomass)
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #



    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # Dario says we will retain this check, but we will make it a warning rather than an error (7/19/2023)

    # Jordan - Biomass - Compare "<0.01 kg" records with "Composite Weight" records to make sure they make sense.
    # errorLog('Compare <0.01 kg records with Composite Weight records to make sure they make sense.')
    single_lt_stations = less_than_records.groupby('stationid').size().reset_index(name='stationcount')
    single_lt_stations = single_lt_stations[single_lt_stations.stationcount == 1]

    if not single_lt_stations.empty:
        
        # issue a warning if 'Only one <0.01 kg record submitted but accompanying composite weight is more than 0.01 kg.'
        print("issue a warning if 'Only one <0.01 kg record submitted but accompanying composite weight is more than 0.01 kg.'")
        
        badrows = trawlinvertebratebiomass[
            (trawlinvertebratebiomass.stationid.isin(single_lt_stations.stationid.tolist()))
            & (trawlinvertebratebiomass.invertspecies.str.lower() == 'composite weight')
            & (trawlinvertebratebiomass.biomass > 0.01)
        ].tmp_row.tolist()
   
        trawlinvertebratebiomass_args.update({
            "badrows": badrows,
            "badcolumn": "stationid,invertspecies,biomass",
            "error_type": "Undefined Error",
            "error_message": 'Only one <0.01 kg record submitted for this station, but the accompanying composite weight is more than 0.01 kg.'
        })
        warnings = [*warnings, checkData(**trawlinvertebratebiomass_args)] 
        
    # checkData(bm.tmp_row.tolist(),'Species/BiomassQualifier/Biomass','Undefined Error','error','Only one <0.01 kg record submitted but accompanying composite weight is more than 0.01 kg.',biomass)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #
    
    # Dario says we will retain this check, but we will make it a warning rather than an error (7/19/2023 1PM)

    # Jordan/Kristin - Cross table checks - abundance vs. biomass  Check to make sure that total amount of records in biomass table is one more than abundance table. If not, make sure the reason makes sense.
    
    # Robert 7/19/2023 4PM - I can see what this is doing is checking that the number of unique taxa in the biomass table is one more than that of abundance
    #     This is covered by the logic check above, therefore this below check will be excluded in bight 2023

    #Get list of different stations in df
    # stid = abundance.stationid.unique()
    # for station in stid:
    #     if abs(len(biomass[biomass['stationid'] == station].invertspecies.unique()) - len(abundance[abundance['stationid'] == station].invertspecies.unique())) > 1 :
    #         checkData(biomass[biomass['stationid'] == station].index.tolist(), 'InvertSpecies','biomass error','error','Biomass records were either too great or too small compared to abundance records',biomass)
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 7b 
    # Biomass units must be kg
    trawlinvertebratebiomass_args.update({
        "badrows": trawlinvertebratebiomass[trawlinvertebratebiomass.biomassunits.astype(str) != 'kg'].tmp_row.tolist(),
        "badcolumn": "BiomassUnits",
        "error_type": "Value Error",
        "error_message": 'Biomass units must be kg'
    })
    errs = [*errs, checkData(**trawlinvertebratebiomass_args)]




    ## END BIOMASS ONLY CHECKS ##
    ## END CUSTOM CHECKS ##
    print("## END BIOMASS ONLY CHECKS ##")
    print("## END CUSTOM CHECKS ##")
        
    return {'errors': errs, 'warnings': warnings}
