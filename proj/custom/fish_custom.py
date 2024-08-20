# Dont touch this file! This is intended to be a template for implementing new custom checks

from inspect import currentframe
from flask import current_app, g
from .functions import checkData, mismatch
import pandas as pd
import numpy as np

def fish(all_dfs):
    print("Start Fish Custom Checks")
    
    current_function_name = str(currentframe().f_code.co_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""

    # define errors and warnings list
    errs = []
    warnings = []

    # since often times checks are done by merging tables (Paul calls those logic checks)
    # we assign dataframes of all_dfs to variables and go from there
    # This is the convention that was followed in the old checker
    
    # This data type should only have tbl_example
    # example = all_dfs['tbl_example']

    # Alter this args dictionary as you add checks and use it for the checkData function
    # for errors that apply to multiple columns, separate them with commas
    # args = {
    #     "dataframe": example,
    #     "tablename": 'tbl_example',
    #     "badrows": [],
    #     "badcolumn": "",
    #     "error_type": "",
    #     "is_core_error": False,
    #     "error_message": ""
    # }

    # Example of appending an error (same logic applies for a warning)
    # args.update({
    #   "badrows": df[df.temperature != 'asdf'].index.tolist(),
    #   "badcolumn": "temperature",
    #   "error_type" : "Not asdf",
    #   "error_message" : "This is a helpful useful message for the user"
    # })
    # errs = [*errs, checkData(**args)]

    # return {'errors': errs, 'warnings': warnings}

    trawlfishabundance = all_dfs['tbl_trawlfishabundance']
    trawlfishbiomass = all_dfs['tbl_trawlfishbiomass']

    trawlfishabundance['tmp_row'] = trawlfishabundance.index
    trawlfishbiomass['tmp_row'] = trawlfishbiomass.index


    trawlfishabundance_args = {
        "dataframe": trawlfishabundance,
        "tablename": 'tbl_trawlfishabundance',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    trawlfishbiomass_args = {
        "dataframe": trawlfishbiomass,
        "tablename": 'tbl_trawlfishbiomass',
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }
    eng = g.eng

    # 1a. Logic check - each record in the trawlfish abundance and biomass has to have a corresponding record in the tbl_trawlevent #
    print("Fish Custom Checks")
    print("each record in the trawlfish abundance and biomass has to have a corresponding record in the tbl_trawlevent")
    matchcols = ['stationid','sampledate','samplingorganization','trawlnumber']
    trawlevent = pd.read_sql("SELECT stationid,sampledate,samplingorganization,trawlnumber FROM tbl_trawlevent;", eng)
    
    trawlfishabundance_args.update({
        "badrows": mismatch(trawlfishabundance, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlfishabundance must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]
    
    # 1b
    trawlfishbiomass_args.update({
        "badrows": mismatch(trawlfishbiomass, trawlevent, matchcols),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in trawlfishbiomass must have a corresponding record in tbl_trawlevent. Records are matched based on {', '.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]


    # ----------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 2. Check abundance and biomass dataframes against the field assignment table
    print("Return error if abundance records are not found in field assignment table")

    # Get the FAT records
    fat = pd.read_sql("""SELECT stationid,assigned_agency AS trawlagency FROM field_assignment_table WHERE "parameter" = 'trawl';""", eng)
    unique_fat_records = [] if fat.empty else fat.apply(lambda row: (row.stationid, row.trawlagency), axis = 1).tolist()
    
    # 2a
    print("Fish Custom Checks")
    print("compare biomass records to field assignment table records (compare on stationid,samplingorganization).")
    # Logic check - compare biomass records to field assignment table records (compare on stationid,samplingorganization).
    # same check exists for abundance
    badrows = trawlfishabundance[
        trawlfishabundance[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].tmp_row.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]

    # 2b
    print("Fish Custom Checks")
    print("compare biomass records to field assignment table records (compare on stationid,samplingorganization).")
    # Logic check - compare biomass records to field assignment table records (compare on stationid,samplingorganization).
    # same check exists for abundance
    badrows = trawlfishbiomass[
        trawlfishbiomass[['stationid','samplingorganization']].apply(lambda x: (x.stationid,x.samplingorganization) not in unique_fat_records, axis=1)
    ].tmp_row.tolist()
    trawlfishbiomass_args.update({
        "badrows": badrows,
        "badcolumn": "StationID,SamplingOrganization",
        "error_type": "Undefined Error",
        "error_message": "You have submitted stations that are not bight stations or were not assigned to your organization."
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 3a
    print("Fish Custom Checks")
    print("Logic checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records.")
    # Logic checks - abundance vs. biomass  Link both abundance and biomass submissions and run mismatch query to check for orphan records. 
    #    "Composite weight" should be only mismatch.  Error message - Orphan records for biomass vs abundance.
    # 
    matchcols = ['stationid','sampledate','samplingorganization','fishspecies']
    trawlfishbiomass_args.update({
        "badrows": mismatch(
            trawlfishbiomass[~trawlfishbiomass.fishspecies.str.lower().isin(['composite weight'])], 
            trawlfishabundance[~trawlfishabundance.fishspecies.str.lower().isin(['composite weight'])], 
            matchcols
        ),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in biomass must match a record in abundance - records are matched based on {','.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]
    
    # 3b
    # Check for those in abundance but not in biomass
    trawlfishabundance_args.update({
        "badrows": mismatch(
            trawlfishabundance[~trawlfishabundance.fishspecies.str.lower().isin(['composite weight'])], 
            trawlfishbiomass[~trawlfishbiomass.fishspecies.str.lower().isin(['composite weight'])], 
            matchcols
        ),
        "badcolumn": ",".join(matchcols),
        "error_type": "Logic Error",
        "error_message": f"Each record in abundance must match a record in biomass - records are matched based on {','.join(matchcols)}"
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 4a
    # Abundance Checks
    # User is required to enter an anomaly, but multple anomalies are allowed to be entered
    print("Fish Custom Checks")
    print("User is required to enter an anomaly, but multple anomalies are allowed to be entered")
    badrows = trawlfishabundance[
        trawlfishabundance.anomaly.apply(
            lambda x: 
            not set([substring.strip() for substring in str(x).split(',')]).issubset(set(pd.read_sql("SELECT DISTINCT anomaly FROM lu_fishanomalies", eng).anomaly.tolist()))
        )
    ].tmp_row.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Anomaly",
        "error_type": "Lookup Error",
        "error_message": f"At least one of the anomalies entered here was not found in the Fish Anomalies <a href=/{current_app.script_root}/scraper?action=help&layer=lu_fishanomalies target=_blank>lookup list</a>. Keep in mind that multiple anomalies must be separated by commas."
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]
    
    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 5a
    # Check species abundance totals and corresponding anomalies - warn if abundance for a fish with an anomaly is greater than one
    print("# Check species abundance totals and corresponding anomalies - warn if abundance for a fish with an anomaly is greater than one")
    badrows = trawlfishabundance[
        (trawlfishabundance.anomaly != 'None') & (trawlfishabundance.abundance > 1)
    ].tmp_row.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Abundance",
        "error_type": "Undefined Warning",
        "error_message": "There is an anomaly here, but the abundance is greater than 1. Just sure to not combine abundance totals of normal organisms with those having anomalies."
    })
    warnings.append(checkData(**trawlfishabundance_args))

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 6a
    # Check for single records that contain anomalies
    print("# Check for single records that contain anomalies")

    # group by stationid, sampledate, samplingorganization, and trawlnumber and fishspecies
    # basically the idea is that if there is a record where the species has an anomaly, 
    #    there should be another record for the same species where there are no anomalies
    # its rare to catch the fish and all of them have anomalies
    # Perform groupby operation
    tmpgroupingcols = ['stationid', 'sampledate', 'samplingorganization', 'trawlnumber', 'fishspecies', 'sizeclass']
    grouped = trawlfishabundance.groupby(tmpgroupingcols).agg({
            'anomaly' : ( lambda anom: (anom.astype(str) != 'None').all() ), # identify groups where all records were anomalies
            'tmp_row' : list
        }).reset_index()

    badgroups = grouped[grouped.anomaly]

    if not badgroups.empty:
        
        badrows = [rownumber for tmplist in badgroups.tmp_row.tolist() for rownumber in tmplist]
        trawlfishabundance_args.update({
            "badrows": badrows,
            "badcolumn": "Anomaly",
            "error_type": "Undefined Warning",
            "error_message": "All organisms of this species and sizeclass at this location were found to have anomaies. Be sure to not combine abundance totals of normal organisms with those having anomalies."
        })
        warnings.append(checkData(**trawlfishabundance_args))


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 7a
    # User must not enter the anomaly "None" along with another anomaly
    # Logic of the code is, if the string "None" is in the anomaly column, then the value in that column needs to be equal to "None"
    # Otherwise it means they entered another value in addition to "None" and that is not allowed
    # badrows = trawlfishabundance[trawlfishabundance.anomaly.str.contains("none", case = False) & (trawlfishabundance.anomaly.str.lower() != 'none')].tmp_row.tolist()
    badrows = trawlfishabundance[
        trawlfishabundance.anomaly.str.contains("none", case=False) & 
        trawlfishabundance.anomaly.str.contains(",")
    ].tmp_row.tolist()

    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Anomaly",
        "error_type": "Value Error",
        "error_message": "You are not allowed to enter 'None' as an anomaly along with other anomaly values - normal organisms cannot be mixed with those with anomalies in the abundance count"
    })
    errs.append(checkData(**trawlfishabundance_args))

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 8a
    print("Fish Custom Checks")
    print("Comment required for anomalies Skeletal, Tumor or Lesion")
    badrows = trawlfishabundance[
        trawlfishabundance[['anomaly','comments']] \
            .replace(np.NaN,'').replace(pd.NA,'').apply(
                lambda x: 
                ( 
                    len(set([s.strip() for s in str(x.anomaly).split(',')]).intersection(set(['Deformity (Skeletal)','Tumor','Lesion']))) > 0
                )
                &
                (
                    str(x.comments) == ''
                ), 
                axis=1
            )
        ] \
        .tmp_row.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "Comments",
        "error_type": "Undefined Error",
        "error_message": "A comment is required for records that have anomalies 'Deformity (Skeletal)', 'Tumor', or 'Lesion'"
    })
    errs = [*errs, checkData(**trawlfishabundance_args)]

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 9a
    # Jordan - Range check - Group by fish species and get the size class ranges (both the min & max). Compare to lu_fishspeciesdepthrange table and minimumdepth/maximumdepth fields.
    print("Fish Custom Checks")
    print("Range check - Group by fish species and get the size class ranges (both the min & max). Compare to lu_fishspeciesdepthrange table and minimumdepth/maximumdepth fields.")
    lu_sizeranges = eng.execute("SELECT scientificname as fishspecies,maximumsizeclass FROM lu_fishspeciesdepthrange;")
    size_ranges = pd.DataFrame(lu_sizeranges.fetchall())
    size_ranges.columns = lu_sizeranges.keys()
    # check that submitted sizeclass is within range on lookuplist
    svr = trawlfishabundance[['fishspecies','sizeclass','tmp_row']].reset_index().merge(size_ranges,on='fishspecies').set_index('index')
    badrows = svr[(svr.sizeclass>svr.maximumsizeclass)&(svr.fishspecies.isin(size_ranges[size_ranges.maximumsizeclass != -88].fishspecies.tolist()))].tmp_row.tolist()
    trawlfishabundance_args.update({
        "badrows": badrows,
        "badcolumn": "FishSpecies, SizeClass",
        "error_type": "Range Error",
        "error_message": f"The size class for these fish are above the maximum recorded. Please verify the species and size class are correct. Check <a href=/{current_app.script_root}/scraper?action=help&layer=lu_fishspeciesdepthrange target=_blank>lu_fishspeciesdepthrange</a> for more information."
    })
    
    # changed to a warning on Dec 15 2023 per request from Dario
    warnings = [*warnings, checkData(**trawlfishabundance_args)]

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #

    # 10a - check the depth range 
    # Jordan - Range check - Check depth ranges (min & max) for all fish species.

    # NOTE from Robert 7/13/2023 - This check applies to biomass as well, but all species in biomass must be in abundance and vice versa
    # if the depth range check is applied to abundance, it automatically applies to biomass
    # In the bight 2018 checker, the check is only written for abundance

    # ANOTHER NOTE: we need the updated fishspecies depth ranges and lookup lists


    print("Fish Custom Checks")
    print("Range check - Check depth ranges (min & max) for all fish species.")
    # 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent
    print("# 1st. Get StartDepth and EndDepth for each unique StationID/SampleDate/SamplingOrganization record from tbl_trawlevent")
    td = pd.read_sql("SELECT stationid,sampledate,samplingorganization,startdepth,enddepth FROM tbl_trawlevent;",eng)
    print('td')
    print(td)
    
    # 2nd. Get Min and Max Depths for each Species from lu_fishspeciesdepthrange
    print("# 2nd. Get Min and Max Depths for each Species from lu_fishspeciesdepthrange")
    depth_ranges = pd.read_sql("SELECT scientificname as fishspecies,minimumdepth,maximumdepth FROM lu_fishspeciesdepthrange;", eng)
    
    # 3rd. Merge Trawl Depth Records and Submitted Fish Abundance Records on StationID/SampleDate/SamplingOrganization
    print("# 3rd. Merge Trawl Depth Records and Submitted Fish Abundance Records on StationID/SampleDate/SamplingOrganization")
    tam = trawlfishabundance[['stationid','sampledate','samplingorganization','fishspecies','tmp_row']].merge(depth_ranges,on='fishspecies').merge(td,on=['stationid','sampledate','samplingorganization'])
    print("Done merging")
    
    print("tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1)")
    print(tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1))
    
    if not tam.empty:
        tam['inrange'] = tam.apply(lambda x: not ((max(x.startdepth,x.enddepth)<x.minimumdepth)|(min(x.startdepth,x.enddepth)>x.maximumdepth)), axis = 1)
        print("Done creating inrange column")

        print("tam")
        print(tam)
        for i in tam[tam.inrange == False].index.tolist():
            # The way the check is written, is so that each species gets its own error message
            # So badrows will always be an individual value in this case
            # So it is an integer. Therefore we need to put it in a list so it can work with the checkData function
            badrows = [tam.iloc[i].tmp_row.tolist()]
            trawlfishabundance_args.update({
                "badrows": badrows,
                "badcolumn": "FishSpecies",
                "error_type": "Undefined Warning",
                "error_message": '{} was caught in a depth range ({}m - {}m) that does not include the range it is typically found ({}m - {}m). Please verify the species is correct. Check <a href=/{}/scraper?action=help&layer=lu_fishspeciesdepthrange target=_blank>lu_fishspeciesdepthrange</a> for more information.'.format(tam.fishspecies[i],int(tam.startdepth[i]),int(tam.enddepth[i]),tam.minimumdepth[i],tam.maximumdepth[i],current_app.script_root)
            })
            warnings = [*warnings, checkData(**trawlfishabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    # 11a
    # Error if Composite weight is found in abundance tab
    
    badrows = trawlfishabundance[trawlfishabundance.fishspecies.str.lower() == 'composite weight'].tmp_row.tolist()
    trawlfishabundance_args = {
        "dataframe": trawlfishabundance,
        "tablename": 'tbl_trawlfishabundance',
        "badrows": badrows,
        "badcolumn": "fishspecies",
        "error_type": "Value Error",
        "is_core_error": False,
        "error_message": "Composite weight cannot be a species in the abundance tab"
    }
    errs = [*errs, checkData(**trawlfishabundance_args)]


    # ---------------------------------------------------------------------------------------------------------------------------------------------------------- #


    
    # Biomass checks

    # 4b
    print("Fish Custom Checks")
    print("If it was submitted as 0 it should rather be submitted as <0.01kg")
    # If it was submitted as 0 it should rather be submitted as <0.01kg
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[ (trawlfishbiomass.biomass < 0.01) & (trawlfishbiomass.biomass != -88) ].tmp_row.tolist(),
        "badcolumn": "Biomass",
        "error_type": "Undefined Error",
        "error_message": 'Any weight less than 0.01kg should be submitted as <0.01kg (0.01 in the biomass column, "<" in the biomassqualifier column)'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]


    # NOTE 1/2/2024
    # 5b commented out because it is redundant with 4b
    # with new addition of allowing -88's, it is actually an incorrect check that will wrongly bar submissions
    # # 5b
    # print("Fish Custom Checks")
    # print("If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.01")
    # # If biomass was measured with greater resolution than what is required in the IM plan ( only one decimal place is allowed), data should be rounded to the nearest 0.01
    # trawlfishbiomass['biomass'] = [round(trawlfishbiomass['biomass'][x], 2) for x in trawlfishbiomass.index]
    # trawlfishbiomass_args.update({
    #     "badrows": trawlfishbiomass[(trawlfishbiomass['biomass'] < .01 ) & ~(trawlfishbiomass['biomassqualifier'].isin(['<']))].tmp_row.tolist(),
    #     "badcolumn": "Biomass,BiomassQualifier",
    #     "error_type": "Undefined Error",
    #     "error_message": """Biomass values that were less than 0.01 kg (e.g. 0.004 kg) should have been submitted as <0.01 kg (.01 in biomass column, and 'less than' in the biomass qualifier column"""
    # })
    # errs = [*errs, checkData(**trawlfishbiomass_args)]

    

    # 6b
    print("Fish Custom Checks")
    print("if using < qualifier, biomass value should be 0.01 or 0.1.")
    # if using < qualifier, biomass value should be 0.01 or 0.1 .
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[(trawlfishbiomass['biomassqualifier'].isin(['<']))& ~(trawlfishbiomass['biomass'].isin([0.01, 0.1]) )].tmp_row.tolist(),
        "badcolumn": "Biomass,BiomassQualifier",
        "error_type": "Undefined Error",
        "error_message": 'if using < qualifier, biomass value should be 0.01 or 0.1. Units are always kg.'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]

    
    # 7b
    print("Fish Custom Checks")
    print("Check biomass ranges (min&max) for each taxon at each station.")
    # NOTE Need to make sure this is doing what Dario wants it to be doing
    # NOTE Dario says leave for now, although the calculation seems odd (7/19/2023)
    #Kristin - Check biomass ranges (min&max) for each taxon at each station.  Error - Impossibly large/questionable biomass values submitted for low abundances of extremely small taxa
    for spec in trawlfishbiomass.fishspecies.unique():
        badrows = trawlfishbiomass[(trawlfishbiomass.fishspecies == spec)&(trawlfishbiomass.biomass > 2 * sorted(trawlfishbiomass.biomass, reverse = True)[2])].tmp_row.tolist()
        if len(badrows) > 0:
            trawlfishbiomass_args.update({
                "badrows": badrows,
                "badcolumn": "Biomass",
                "error_type": "Undefined Warning",
                "error_message": "Questionable biomass value for a low abundance species, appears larger then expected. Please check if species size justifies the questionable biomass value."
            })
            warnings = [*warnings, checkData(**trawlfishbiomass_args)]

    # 8b
    # Biomass units must be kg
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[trawlfishbiomass.biomassunits.astype(str) != 'kg'].tmp_row.tolist(),
        "badcolumn": "BiomassUnits",
        "error_type": "Value Error",
        "error_message": 'Biomass units must be kg'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]


    # 9b
    print("Fish Custom Checks")
    print("If biomass is -88, a comment is required")
    # If biomass is -88, a comment is required
    trawlfishbiomass_args.update({
        "badrows": trawlfishbiomass[ (trawlfishbiomass.biomass == -88) & (trawlfishbiomass.comments.fillna('') == '') ].tmp_row.tolist(),
        "badcolumn": "Biomass, Comments",
        "error_type": "Undefined Error",
        "error_message": 'If biomass is -88, a comment is required'
    })
    errs = [*errs, checkData(**trawlfishbiomass_args)]

    # End of fish checks
    print('End of fish checks')

    return {'errors': errs, 'warnings': warnings}
