# this function will be imported into field_trawl, and field_grab, and field_trawl_and_grab
# This way, one function will control the field checks
# This system will be unique to bight, since it is one datatype, with three different possible combinations of dataframes
# We previously had 3 custom checks files with redundant code, but instead, we can do it this way:
#  Make this file with a function that gets imported to those 3 other custom checks files
#  This function can take 3 dataframes as arguments, with default on each set to none. But the occupation dataframe must be required.

import re, os, binascii
from inspect import currentframe
from flask import current_app, g, session
from .functions import checkData, haversine_np, check_distance, check_time, check_strata_grab, check_strata_trawl, export_sdf_to_json, calculate_distance, check_samplenumber_sequence
import pandas as pd
import numpy as np
from arcgis.geometry import Point as arcgisPoint
from arcgis.geometry import Geometry, Polyline
from shapely import wkb
import shapely


# Apologies in advance to whomever takes over this
# The spatial checks had features keep getting added just before data started coming in, and even slightly after
# so the code (mainly the spatial map element) became disorganized - Robert 9/28/2023
def fieldchecks(occupation, eng, trawl = None, grab = None):
    #return {'errors': [], 'warnings': []}
    current_function_name = str(currentframe().f_code.co_name)
    print("current_function_name")
    print(current_function_name)


    # define errors and warnings list
    # These will be returned from the function at the end
    errs = []
    warnings = []

    args = {
        "badrows": [],
        "badcolumn": "",
        "error_type": "",
        "is_core_error": False,
        "error_message": ""
    }

    # Sorry for the weird notation i just wanted this to take fewer lines
    occupation_args = {**args, **{"dataframe": occupation, "tablename": 'tbl_stationoccupation' } }
    trawl_args      = {**args, **{"dataframe": trawl,      "tablename": 'tbl_trawlevent'        } }
    grab_args       = {**args, **{"dataframe": grab,       "tablename": 'tbl_grabevent'         } }

    # I am going to be forcing Karen to match the field assignment table with the strata layer, so we might not need to do that
    # and in fact, doing so may possibly break it in bight 2023
    # it is now June 15, 2023 and i can confirm that it did break it in bight2023 - Robert
    
    eng = g.eng
    
    field_assignment_table = pd.read_sql("SELECT * FROM field_assignment_table", eng)

    noshapestations = pd.read_sql(
        "SELECT fat.stationid, fat.stratum AS fatstrat, sr.stratum, fat.region AS fatregion, sr.region, sr.shape FROM field_assignment_table fat LEFT JOIN strataregion sr ON fat.stratum = sr.stratum AND fat.region = sr.region WHERE sr.shape is null", 
        g.eng
    )
    
    # Initiates the parts needed for strata check
    strata = pd.read_sql("SELECT * FROM strataregion", eng)

    # I noticed that if the grab distance to target is too far, it doesnt get placed on the map
    # the points consist of occupations and grabs, whereas the lines are only trawls
    # The goal is to include spatial dataframes in their own geojson files to send to the browser 
    #   so it can plot them on the map for the user to view
    bad_point_distances = pd.DataFrame()
    bad_line_distances = pd.DataFrame()
    

    # ------- LOGIC CHECKS ------- #
    print("# ------- LOGIC CHECKS ------- #")
    
    # If its a field grab submission, there should be only grab collectiontype records.
    if trawl is None:
        occupation_args.update({
            "badrows": occupation[occupation.collectiontype != 'Grab'].index.tolist(),
            "badcolumn": "CollectionType",
            "error_type" : "Logic Error",
            "error_message" : "This is a Field Occupation/Grab submission, but there are records that do not have a collection type of 'Grab'"
        })
        errs.append(checkData(**occupation_args))
    
    # If its a field trawl submission, there should be only trawl collectiontype records.
    if grab is None:
        occupation_args.update({
            "badrows": occupation[~occupation.collectiontype.isin(['Trawl 5 Minutes', 'Trawl 10 Minutes'])].index.tolist(),
            "badcolumn": "CollectionType",
            "error_type" : "Logic Error",
            "error_message" : "This is a Field Occupation/Trawl submission, but there are records that do not have a collection type of 'Trawl 5 Minutes' or 'Trawl 10 Minutes'"
        })
        errs.append(checkData(**occupation_args))


    if trawl is not None:
        # Check - Each Trawl record must have a corresponding stationoccupation record
        print("# Check - Each Trawl record must have a corresponding stationoccupation record") 
        print("with collection type 'Trawl 5 Minutes' or 'Trawl 10 Minutes'")
        tmpocc = occupation[occupation.collectiontype.isin(['Trawl 5 Minutes', 'Trawl 10 Minutes'])].assign(present = 'yes')
        if not tmpocc.empty:
            tmp = trawl.merge(
                tmpocc, 
                left_on = ['stationid','sampledate','samplingorganization'], 
                right_on = ['stationid','occupationdate','samplingorganization'], 
                how = 'left',
                suffixes = ('','_occ')
            )
            badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": "StationID,SampleDate,SamplingOrganization",
                "error_type" : "Logic Error",
                "error_message" : "Each Trawl record must have a corresponding Occupation record (with a Trawl collectiontype). Records are matched on StationID, SampleDate, and SamplingOrganization."
            })
            errs = [*errs, checkData(**trawl_args)]
        else:
            occupation_args.update({
                "badrows": occupation.index.tolist(),
                "badcolumn": "CollectionType",
                "error_type" : "Logic Error",
                "error_message" : "There are no records with a collectiontype of 'Trawl 5/10 Minutes' although this is a Field Trawl submission"
            })
            errs.append(checkData(**occupation_args))
    
    if grab is not None:
        # Check - Each Grab record must have a corresponding stationoccupation record
        print("# Check - Each Grab record must have a corresponding stationoccupation record")
        print("with collection type Grab")
        tmpocc = occupation[occupation.collectiontype == 'Grab'].assign(present = 'yes')
        if not tmpocc.empty:
            tmp = grab.merge(
                tmpocc, 
                left_on = ['stationid','sampledate','samplingorganization'], 
                right_on = ['stationid','occupationdate','samplingorganization'], 
                how = 'left',
                suffixes = ('','_occ')
            )
            badrows = tmp[pd.isnull(tmp.present)].tmp_row.tolist()
            grab_args.update({
                "badrows": badrows,
                "badcolumn": "StationID,SampleDate,SamplingOrganization",
                "error_type" : "Logic Error",
                "error_message" : "Each Grab record must have a corresponding Occupation record (with a Grab collectiontype). Records are matched on StationID, SampleDate, and SamplingOrganization."
            })
            errs = [*errs, checkData(**grab_args)]
            del tmp
            del badrows
        else:
            occupation_args.update({
                "badrows": occupation.index.tolist(),
                "badcolumn": "CollectionType",
                "error_type" : "Logic Error",
                "error_message" : "There are no records with a collectiontype of 'Grab' although this is a Field Grab submission"
            })
            errs.append(checkData(**occupation_args))



    print("# Check the time formats on all time columns")
    # Check the time formats on all time columns
    def checkTime(df, col, args, time_format = re.compile(r'^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$'), custom_errmsg = None):
        """default to checking the 24 hour clock time"""
        args.update({
            "badrows": df[~df[col.lower()].apply(lambda x: bool(time_format.match(str(x).strip())) )].tmp_row.tolist(),
            "badcolumn": col,
            "error_type" : "Formatting Error",
            "error_message" : f"The column {col} is not in a valid 24 hour clock format (HH:MM:SS)" if not custom_errmsg else custom_errmsg
        })
        return checkData(**args)
    
    print("# Grab and trawl may possibly be NoneTypes")
    # Grab and trawl may possibly be NoneTypes
    time_errs = [
        checkTime(occupation, 'OccupationTime', occupation_args),
        checkTime(trawl, 'OverTime', trawl_args) if trawl is not None else {} ,
        checkTime(trawl, 'StartTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'EndTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'DeckTime', trawl_args) if trawl is not None else {},
        checkTime(trawl, 'OnBottomTime', trawl_args) if trawl is not None else {},
        checkTime(grab, 'SampleTime', grab_args) if grab is not None else {}
    ]
    errs = [
        *errs, 
        *time_errs, 
    ]

    # if no time format errors, then we can check the logic of the grab and trawl numbers
    if all([(len(t) == 0) for t in time_errs]):
        
        if grab is not None:
            
            grab_args.update({
                "badrows": check_samplenumber_sequence(grab, 'sampletime', 'grabeventnumber'),
                "badcolumn": 'GrabEventNumber',
                "error_type" : "Logic Error",
                "error_message" : "GrabEventNumber sequence is incorrect, check GrabEventNumber and SampleTime columns."
            })
            errs.append(checkData(**grab_args))
        
        if trawl is not None:
            
            # Check Logic of Trawl Numbers (Start Time)
            trawl_args.update({
                "badrows": check_samplenumber_sequence(trawl, 'starttime', 'trawlnumber'),
                "badcolumn": 'TrawlNumber',
                "error_type" : "Logic Error",
                "error_message" : "TrawlNumber sequence is incorrect, check SampleDate and StartTime."
            })
            errs.append(checkData(**trawl_args))
            
            # Check Logic of Trawl Numbers (Over Time)
            trawl_args.update({
                "badrows": check_samplenumber_sequence(trawl, 'overtime', 'trawlnumber'),
                "error_message" : "TrawlNumber sequence is incorrect, check SampleDate and OverTime."
            })
            errs.append(checkData(**trawl_args))

    # ------- END LOGIC CHECKS ------- #



    # Run the following checks only if there are no logic errors
    if len(errs) == 0:
        print("# ------- Occupation Checks ------- #")
        # ------- Occupation Checks ------- #
        
        ## Kristin - StationOccupation/Trawl/Grab check DepthUnits field make sure nobody has entered feet instead of meters
        ## (this is an error not a warning). Generic lookup list allows it, but Dario doesnt want users to be able to enter feet. 
        # Depth units should be in meters, not feet
        badrows = (occupation[['occupationdepthunits','tmp_row']].where(occupation['occupationdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
        occupation_args.update({
            "badrows": badrows,
            "badcolumn": 'OccupationDepthUnits',
            "error_type" : "Undefined Error",
            "error_message" : "OccupationDepthUnits should be in meters, not feet"
        })
        errs = [*errs, checkData(**occupation_args)]

        if trawl is not None:
            print("# Depth units should be in meters, not feet")
            # Depth units should be in meters, not feet
            badrows = (trawl[['depthunits','tmp_row']].where(trawl['depthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": 'DepthUnits',
                "error_type" : "Undefined Error",
                "error_message" : "DepthUnits should be in meters, not feet"
            })
            errs = [*errs, checkData(**trawl_args)]
        
        if grab is not None:
            print("# Depth units should be in meters, not feet")
            # Depth units should be in meters, not feet
            badrows = (grab[['stationwaterdepthunits','tmp_row']].where(grab['stationwaterdepthunits'].isin(['ft','f'])).dropna()).tmp_row.tolist()
            grab_args.update({
                "badrows": badrows,
                "badcolumn": 'StationWaterDepthUnits',
                "error_type" : "Undefined Error",
                "error_message" : "DepthUnits should be in meters, not feet"
            })
            errs = [*errs, checkData(**grab_args)]


        print("# Comment required if the station was abandoned")
        # Comment required if the station was abandoned
        badrows = occupation[['abandoned', 'comments','tmp_row']].where(occupation['abandoned'].isin(['Yes'])).dropna(axis = 0, how = 'all').loc[pd.isnull(occupation['comments'])].tmp_row.tolist()
        occupation_args.update({
            "badrows": badrows,
            "badcolumn": 'Comments',
            "error_type" : "Missing Required Data",
            "error_message" : 'A comment is required if the station was abandoned'
        })
        errs = [*errs, checkData(**occupation_args)]

        print("# Comment required for certain stationfail values")
        lu_sf = pd.read_sql("select stationfail from lu_stationfails where UPPER(commentrequired) = 'YES'", eng)
        stationfail_matches = pd.merge(occupation[['stationfail','comments','tmp_row']],lu_sf, on=['stationfail'], how='inner') 
        stationfail_matches['comments'].replace('', pd.NA, inplace=True)

        badrows = stationfail_matches[pd.isnull(stationfail_matches['comments'])].tmp_row.tolist()
        occupation_args.update({
            "badrows": badrows,
            "badcolumn": 'Comments',
            "error_type" : "Missing Required Data",
            "error_message" : "A comment is required for the value you entered in the StationFail field."
        })
        errs = [*errs, checkData(**occupation_args)]


        print("# Make sure agency was assigned to that station for the corresponding collection type - Grab or Trawl")
        print("# There should only be one sampling organization per submission - this is just a warning")
        sampling_organizations = occupation.samplingorganization.unique() 
        if len(sampling_organizations) >= 1:
            if len(sampling_organizations) > 1:
                occupation_args.update({
                    "badrows": occupation[occupation.samplingorganization == occupation.samplingorganization.min()].tmp_row.tolist(),
                    "badcolumn": 'SamplingOrganization',
                    "error_type" : "Undefined Warning",
                    "error_message" : "More than one agency detected"
                })
                warnings = [*warnings, checkData(**occupation_args)]
            
            for organization in sampling_organizations:
                trawlstations = pd.read_sql(f"""SELECT DISTINCT stationid FROM field_assignment_table WHERE "parameter" = 'trawl' AND assigned_agency = '{organization}' ; """, eng).stationid.tolist()
                badrows = occupation[(occupation.collectiontype != 'Grab') & (~occupation.stationid.isin(trawlstations)) & (occupation.samplingorganization == organization)].tmp_row.tolist()
                occupation_args.update({
                    "badrows": badrows,
                    "badcolumn": 'StationID,SamplingOrganization',
                    "error_type" : "Undefined Warning",
                    "error_message" : f"The organization {organization} was not assigned to trawl at this station"
                })
                warnings = [*warnings, checkData(**occupation_args)]
                
                grabstations = pd.read_sql(f"""SELECT DISTINCT stationid FROM field_assignment_table WHERE "parameter" = 'sediment' AND assigned_agency = '{organization}' """, eng).stationid.tolist()
                badrows = occupation[(occupation.collectiontype == 'Grab') & (~occupation.stationid.isin(grabstations)) & (occupation.samplingorganization == organization)].tmp_row.tolist()
                occupation_args.update({
                    "badrows": badrows,
                    "badcolumn": 'StationID,SamplingOrganization',
                    "error_type" : "Undefined Warning",
                    "error_message" : f"The organization {organization} was not assigned to grab at this station"
                })
                warnings = [*warnings, checkData(**occupation_args)]
        else: 
            raise Exception("No sampling organization detected")

        # Not sampling brackish estuaries this bight cycle
        print("# Check StationOccupation/Salinity - if the station is an Estuary or Brackish Estuary then the salinity is required")
        estuaries = pd.read_sql("SELECT DISTINCT stationid, stratum FROM field_assignment_table WHERE stratum IN ('Estuaries');", eng)

        print("# Only run if they submitted data for estuaries")
        # Per Dario June 12 2023 - We are not sampling Brackish Estuaries so only check for Estuaries
        if len((occupation[(occupation.stationid.isin(estuaries.stationid))]))!=0 :
            print("# for matching stationids, make sure Estuary salinity has a value")
            print('## Make sure Estuary salinity value is non-empty ##')
            strats = pd.merge(occupation[['stationid','salinity','tmp_row']],estuaries, how = 'left', on='stationid')
            occupation_args.update({
                "badrows": strats[pd.isnull(strats.salinity)].tmp_row.tolist(),
                "badcolumn": 'Salinity',
                "error_type": 'Undefined Error',
                "error_message": 'Salinity is required for stations that are Estuaries and user must enter -88 if measurement is actually missing.'
            })
            errs = [*errs, checkData(**occupation_args)]



        print("# Jordan - Station Occupation Latitude/Longitude should be no more than 100M from Field Assignment Table Target Latitude/Longitude otherwise warning")
        print("# Merges SO dataframe and FAT dataframe according to StationIDs")
        so = occupation[['stationid','occupationlatitude','occupationlongitude','tmp_row']]
        fat = pd.read_sql("SELECT DISTINCT stationid, latitude AS targetlatitude, longitude AS targetlongitude, region FROM field_assignment_table", eng)
        sofat = pd.merge(so, fat, how = 'left', on ='stationid')

        # Raises Error for Unmatched StationIDs & Distances More than 100M from FAT Target
        print("Raises error for unmatched stationids & distances more than 100m from fat target:")
        occupation_args.update({
            "badrows": sofat[sofat['targetlatitude'].isnull()].tmp_row.tolist(),
            "badcolumn": 'StationID',
            "error_type": 'Logic Error',
            "error_message": 'StationOccupation distance to target check - Could not find StationID in field assignment table.'
        })
        errs = [*errs, checkData(**occupation_args)]


        # Calculates distance between SO Lat/Lon and FAT Lat/Lon according to StationIDs
        print("Calculates distance between so lat/lon and fat lat/lon according to stationids:")

        # Need to specify the subset - it was dropping records it wasnt supposed to before
        sofat.dropna(subset=['targetlatitude','targetlongitude'], inplace=True)
        sofat['targetlatitude'] = sofat['targetlatitude'].apply(lambda x: float(x))
        sofat['targetlongitude'] = sofat['targetlongitude'].apply(lambda x: float(x))
        

        # https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas
        # haversine apparently uses a projection other than WGS84 which may cause small errors but none significant enough to affect this check
        # plus this check is just a warning
        sofat['dists'] = haversine_np(sofat['occupationlongitude'],sofat['occupationlatitude'],sofat['targetlongitude'],sofat['targetlatitude'])

        # Raises Warning for Distances calculated above > 100M
        print("Raises warning for distances calculated above > 100m")


        bad_sofat_dists_df = sofat[
            sofat.apply(
                lambda row: row['dists'] > (200 if 'channel islands' in str(row['region']).strip().lower() else 100),
                axis = 1
            )
        ]

        if not bad_sofat_dists_df.empty:
            # append to bad_point distances
            bad_point_distances = pd.concat(
                [
                    bad_point_distances, 
                    bad_sofat_dists_df[['stationid','dists']].assign(
                        error_message = "Distance from Occupation Latitude/Longitude in submission to Target Latitude/Longitude in field assignment table is greater than 100 meters. (up tp 200m is ok for channel islands stations)",
                        SHAPE = bad_sofat_dists_df.apply(lambda x: arcgisPoint({"x": x['occupationlongitude'], "y": x['occupationlatitude'], "spatialReference": {"wkid": 4326}}), axis=1)  
                    ).rename(columns={'dists':'distance_to_target'})
                ]
            )

            bad_point_distances.distance_to_target = bad_point_distances.distance_to_target.apply(
                lambda x: np.round(x, 2)
            )

            occupation_args.update({
                "badrows":  bad_sofat_dists_df.tmp_row.tolist(),
                "badcolumn": 'StationID',
                "error_type": 'Undefined Warning',
                "error_message": 'Distance from Occupation Latitude/Longitude in submission to Target Latitude/Longitude in field assignment table is greater than 100 meters. (up tp 200m is ok for channel islands stations)'
            })
            warnings = [*warnings, checkData(**occupation_args)]
        

        # Matthew M- If StationOccupation/Station Fail != "None or No Fail/Temporary" then Abandoned should be set to "Yes"
        #- Message should read "Abandoned should be set to 'Yes' when Station Fail != 'None or No Fail' or 'Temporary'" 
        print("If StationOccupation/Station Fail != None or No Fail/Temporary then Abandoned should be set to Yes")
        results= eng.execute("select lu_stationfails.stationfail from lu_stationfails")
        lu_sf1 = pd.DataFrame(results.fetchall())
        lu_sf1.columns = results.keys()
        lu_sf1.columns = [x.lower() for x in lu_sf1.columns]
        lu_sf1=lu_sf1.stationfail[lu_sf1.stationfail.str.contains('None or No Failure|Temporary|Temporarily ', case=False, na=False)]
        
        occupation_args.update({
            "badrows": occupation[(~occupation.stationfail.isin(lu_sf1.tolist())) & ~occupation['abandoned'].isin(['Yes', 'yes'])].tmp_row.tolist(),
            "badcolumn": 'StationFail',
            "error_type": 'Undefined Error',
            "error_message": 'If StationOccupation/StationFail is set to anything other than "None or No Failure" or Temporary then Abandoned should be set to Yes.'
        })
        errs = [*errs, checkData(**occupation_args)]

        #2nd case check If StationOccupation/Station Fail = "None or No Fail/Temporary" then Abandoned should be set to "No"
        #- Message should read "Abandoned should be set to "No" when Station Fail is None or No Failure or Temporary"
        print("If StationOccupation/Station Fail = None or No Fail/Temporary then Abandoned should be set to No")
        occupation_args.update({
            "badrows": occupation[(occupation.stationfail.isin(lu_sf1.tolist())) & occupation['abandoned'].isin(['Yes', 'yes'])].tmp_row.tolist(),
            "badcolumn": 'StationFail',
            "error_type": 'Undefined Error',
            "error_message": 'If StationOccupation/StationFail is set to "None or No Failure" or Temporary then Abandoned should be set to No.'
        })
        errs = [*errs, checkData(**occupation_args)]

        # StationOccupation check SalinityUnits must be either 'ppt' or 'psu'
        print("# StationOccupation check SalinityUnits must be either 'ppt' or 'psu'")
        occupation_args.update({
            "badrows": occupation[~occupation.salinityunits.isin(['ppt', 'psu'])].tmp_row.tolist(),
            "badcolumn": 'SalinityUnits',
            "error_type": 'Undefined Error',
            "error_message": 'SalinityUnits must be either ppt or psu.'
        })
        errs = [*errs, checkData(**occupation_args)]

        # Check - If Datum is Other, then a comment is required.
        print("# Check - If Datum is Other, then a comment is required.")
        occupation_args.update({
            "badrows": occupation[(occupation['occupationdatum'] == 'Other (comment required)') & (pd.isnull(occupation['comments']))].tmp_row.tolist(),
            "badcolumn": 'Comments',
            "error_type": 'Undefined Error',
            "error_message": 'If Datum is Other, then a comment is required.'
        })
        errs = [*errs, checkData(**occupation_args)]




        # Begin routine to check if the field assignment table is set up correctly for their submission
        HAS_REGION_GEOMETRY = False

        # Check if stationids are in field_assignment_table
        merged = pd.merge(
            pd.concat( 
                [
                    occupation[['stationid']], 
                    trawl[['stationid']] if trawl is not None else pd.DataFrame(), 
                    grab[['stationid']] if grab is not None else pd.DataFrame()
                ] , 
                ignore_index = True
            ),
            field_assignment_table.filter(items=['stationid','stratum','region']).drop_duplicates(), 
            how='left', 
            on=['stationid'],
            indicator=True
        )
        bad_df = merged[merged['_merge'] == 'left_only']
        badrows = occupation[occupation.stationid.isin(noshapestations.stationid)].tmp_row.tolist()
        
        if len(bad_df) > 0:
            occupation_args.update({
                "badrows": bad_df.tmp_row.tolist(),
                "badcolumn": 'stationid',
                "error_type": "Lookup Error",
                "error_message" : f'These stations are not in the field assignment table, contact b23-im@sccwrp.org for assistance'
            })
            errs = [*errs, checkData(**occupation_args)]
            HAS_REGION_GEOMETRY = False
        else:
            HAS_REGION_GEOMETRY = True

        if len(badrows) > 0:
            occupation_args.update({
                "badrows": badrows,
                "badcolumn": 'stationid',
                "error_type": "Lookup Error",
                "error_message" : f'These stations do not have geometry in the field assignment table, contact b23-im@sccwrp.org for assistance'
            })
            errs = [*errs, checkData(**occupation_args)]
            HAS_REGION_GEOMETRY = False
        else:
            HAS_REGION_GEOMETRY = True

        



        ### END OCCUPATION CHECKS ###
        if trawl is not None:
            # ------- Trawl Checks ------- #
            # Eric Hermoso - (TrawlOverDistance) - Distance from net start (Trawl/OverLat/OverLon) to end (Trawl/StartLat/StartLon) in meters.
            print('New calculated field (TrawlOverDistance) - Distance from net start (Trawl/OverLat/OverLon) to end (Trawl/StartLat/StartLon) in meters.')
            trawl['trawloverdistance'] = check_distance(trawl,trawl['overlatitude'],trawl['startlatitude'],trawl['overlongitude'],trawl['startlongitude'])

            # Eric Hermoso - (TrawlDeckDistance) - Distance from end (Trawl/EndLat/EndLon) to on-deck (Trawl/DeckLat/DeckLon) in meters)
            print('New calculated field (TrawlDeckDistance)')
            trawl['trawldeckdistance'] = check_distance(trawl,trawl['endlatitude'],trawl['decklatitude'],trawl['endlongitude'],trawl['decklongitude'])

            # Kristin - New calculated field (TrawlDistance) - Distance from start (Trawl/StartLat/StartLon) to end (Trawl/EndLat/EndLon) in meters.
            print('New calculated field (TrawlDistance)')
            trawl['trawldistance'] = check_distance(trawl,trawl['startlatitude'],trawl['endlatitude'],trawl['startlongitude'],trawl['endlongitude'])

            # (TrawlOverTime) - Time between net start (Trawl/OverDate) to end (Trawl/StartDate) in meters
            print('New calculated field (TrawlOverTime) - Time between net start (Trawl/OverDate) to end (Trawl/StartDate) in meters')
            trawl['trawlovertime'] = check_time(trawl['overtime'].map(str),trawl['starttime'].map(str))

            # (TrawlDeckTime) - Time between end (Trawl/EndDate) to on-deck (Trawl/DeckDate) in meters
            print('New calculate field (TrawlDeckTime) - Time between end (Trawl/EndDate) to on-deck (Trawl/DeckDate) in meters')
            trawl['trawldecktime'] = check_time(trawl['endtime'].map(str),trawl['decktime'].map(str))

            # (TrawlTimeToBottom) - Distance from (Trawl/WireOut) divided by calculated field OverDistance
            print('New calculate field (TrawlTimeToBottom)')
            # If trawl['wireout'] is equal to 0, changes to -88 so that trawltimetobottom is numeric and not null
            trawl['wireout'] = [ -88 if trawl['wireout'][x]== 0 else trawl['wireout'][x] for x in trawl.index]
            trawl['trawltimetobottom'] = trawl['trawloverdistance']/trawl['wireout']

            # Kristin - New calculated field (TrawlTime) - Time difference from start (Trawl/StartDate) to end (Trawl/EndDate).
            print('before trawltime')
            trawl['trawltime'] = check_time(trawl.starttime.map(str), trawl.endtime.map(str))



            # Eric Hermoso - Check that both Trawl/StartDepth and Trawl/EndDepth are no more than 10% off of StationOccupation (Depth) - warning only 
            # lets just get actual trawls from station occupation - we may to adjust this further to only get successful trawls
            print("## Check that both Trawl/StartDepth and Trawl/EndDepth are no more than 10% off of StationOccupation (Depth) - warning only ##")
            occupation_trawls = occupation[['stationid','occupationdepth','collectiontype']].where(occupation['collectiontype'].isin(['Trawl 10 Minutes','Trawl 5 Minutes']))
            print("occupation_trawls")
            print(occupation_trawls)
            # drop emptys
            occupation_trawls = occupation_trawls.dropna()
            # now we get the correct number of trawls
            merge_trawl_occupation = pd.merge(occupation_trawls[['stationid','occupationdepth']], trawl[['stationid','startdepth','enddepth','tmp_row']], how = 'right', on = 'stationid')
            print("merge_trawl_occupation")
            print(merge_trawl_occupation)
            print("## Trawl start depth is greater than 10 percent of occupation depth ##")
            trawl_args.update({
                "badrows": merge_trawl_occupation.loc[(abs(merge_trawl_occupation['occupationdepth'] - merge_trawl_occupation['startdepth'])/merge_trawl_occupation['startdepth']*100) > 10].tmp_row.tolist(),
                "badcolumn": "StartDepth",
                "error_type": "Undefined Warning",
                "error_message" : 'Trawl start depth is greater than 10 percent of occupation depth.'
            })
            warnings = [*warnings, checkData(**trawl_args)]

            print("## Trawl end depth is greater than 10 percent of occupation depth ##")
            badrows = [int(x) for x in merge_trawl_occupation.loc[(abs(merge_trawl_occupation['occupationdepth'] - merge_trawl_occupation['enddepth'])/merge_trawl_occupation['enddepth']*100) > 10 ].tmp_row.unique()]
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": "EndDepth",
                "error_type": "Undefined Warning",
                "error_message" : 'Trawl end depth is greater than 10 percent of occupation depth.'
            })
            warnings = [*warnings, checkData(**trawl_args)]

            # 2 - Kristin - 
            # If its a 10 minute trawl the distance should be greater than 650 meters otherwise warn.
            # If its a 5 minute trawl the distance should be greater than 325 meters otherwise warn.
            # If 10 minute trawl is greater than 16 minutes or less than 8 then warning.
            # If 5 minute trawl is greater than 8 minutes or less than 4 then warning.
            # use occupation trawls from check above - has the same content you need
            trawl_occupation_time = pd.merge(occupation_trawls[['stationid','collectiontype']], trawl[['stationid','trawltime','trawldistance','tmp_row']], how = 'right', on = 'stationid')
            print("## CHECK 10 MINUTE TRAWL THE DISTANCE SHOULD BE GREATER THAN 650 METERS ##")
            print(trawl_occupation_time)
            trawl_args.update({
                "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 10 Minutes')&(trawl_occupation_time['trawldistance']< 650)].tmp_row.tolist(),
                "badcolumn": 'StartLatitude,StartLongitude,EndLatitude,EndLongitude',
                "error_type": "Undefined Warning",
                "error_message" : 'A 10 minute trawl should be greater than 650 m'
            })
            warnings = [*warnings, checkData(**trawl_args)]
            
            print("## CHECK 5 MINUTE TRAWL THE DISTANCE SHOULD BE GREATER THAN 325 METERS ##")
            trawl_args.update({
                "badrows": trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 5 Minutes')&(trawl_occupation_time['trawldistance'] < 325)].tmp_row.tolist(),
                "badcolumn": 'StartLatitude,StartLongitude,EndLatitude,EndLongitude',
                "error_type": "Undefined Warning",
                "error_message" : 'A 5 minute trawl should be greater than 325 m'
            })
            warnings = [*warnings, checkData(**trawl_args)]
            
            print("## CHECK 10 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 16 MINUTES OR SHORTER THAN 8 ##")
            badrows = [int(x) for x in trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 10 Minutes')&((trawl_occupation_time['trawltime'] < 8)|(trawl_occupation_time['trawltime'] > 16))].tmp_row.unique()]
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": 'StartTime,EndTime',
                "error_type": "Undefined Warning",
                "error_message" : 'A 10 minute trawl should be between 8 and 16 minutes'
            })
            warnings = [*warnings, checkData(**trawl_args)]
            
            print("## CHECK 5 MINUTE TRAWL SHOULD NOT RUN LONGER THAN 8 MINUTES OR SHORTER THAN 4 MINUTES ##")
            badrows = [int(x) for x in trawl_occupation_time.loc[(trawl_occupation_time['collectiontype']=='Trawl 5 Minutes')&((trawl_occupation_time['trawltime'] < 4)|(trawl_occupation_time['trawltime']> 8))].tmp_row.unique()]
            trawl_args.update({
                "badrows": badrows,
                "badcolumn": 'StartTime,EndTime',
                "error_type": "Undefined Warning",
                "error_message" : 'A 5 minute trawl should be between 4 and 8 minutes'
            })
            warnings = [*warnings, checkData(**trawl_args)]

            ##  New calculated field (TrawlDistanceToNominalTarget) - Draw a line from StartLat/StartLon to EndLat/Lon calculate nearest point to tblStations Lat/Lon.
            trawlstations = ",".join( [f""" '{s}' """ for s in trawl.stationid.unique()] )
            fat = pd.read_sql(
                f"SELECT stationid, latitude AS targetlatitude, longitude AS targetlongitude, stratum, region FROM field_assignment_table WHERE stationid IN ({trawlstations})", 
                g.eng
            )
            checkdf = trawl.merge(fat, on = ['stationid'], how = 'left')
            trawl_args.update({
                "badrows": checkdf[checkdf.targetlatitude.isnull() | checkdf.targetlongitude.isnull() ].tmp_row.tolist(),
                "badcolumn": 'StationID',
                "error_type": "Undefined Error",
                "error_message" : f'StationID not found in the <a href={current_app.script_root}/scraper?action=help&layer=vw_field_assignment>field assignment table</a>'
            })
            errs = [*errs, checkData(**trawl_args)]

            # remove rows with missing lat longs
            checkdf = checkdf.dropna(subset = ['targetlatitude','targetlongitude'])

            # Now time to check the distance of the trawl line to the target station
            # the calculate_distance function returns the value in meters
            checkdf['dist'] = checkdf.apply(calculate_distance, axis=1)

            checkdf['max_allowable_distance'] = checkdf.region.apply(
                lambda x: 
                200 if ('channel islands' in str(x).lower()) else 100
            )

            # see if they passed or not
            checkdf['passed'] = checkdf.apply(lambda row: row.dist <= row.max_allowable_distance, axis = 1 )
            
            # get the bad records that didnt pass
            checkdf = checkdf[~checkdf.passed]


            if not checkdf.empty:
                # append to bad_line distances to put on the map later
                bad_line_distances = pd.concat(
                    [
                        bad_line_distances, 
                        checkdf[['stationid','dist']].assign(
                            error_message = "Distance from Occupation Latitude/Longitude in submission to Target Latitude/Longitude in field assignment table is greater than 100 meters. (up tp 200m is ok for channel islands stations)",
                            SHAPE = checkdf.apply(
                                lambda row: Polyline({
                                    "paths": [[[row['startlongitude'], row['startlatitude']], [row['endlongitude'], row['endlatitude']]]],
                                    "spatialReference": {"wkid": 4326}
                                }),
                                axis=1
                            )
                        ).rename(columns={'dist':'distance_to_target'}) 
                    ]
                )

                bad_line_distances.distance_to_target = bad_line_distances.distance_to_target.apply(
                    lambda x: np.round(x, 2)
                )

            # get the bad rows
            checkdf = checkdf.groupby(['region','max_allowable_distance']).agg({
                'tmp_row':list
            }).reset_index()
            
            print("trawl check df, again")
            print(checkdf)

            if not checkdf.empty:
                for _, row in checkdf.iterrows():
                    trawl_args.update({
                        "badrows": row.tmp_row,
                        "badcolumn": 'StartLatitude,StartLongitude,EndLatitude,EndLongitude',
                        "error_type": "Undefined Error",
                        "error_message" : f'Your trawl was in the region {row.region} and the distance from the trawl line to the target station was over {row.max_allowable_distance} meters'
                    })
                    warnings = [*warnings, checkData(**trawl_args)]


            ## Kristin - bug fixed on 26jun18
            ## Check - If PTSensor = Yes then PTSensorManufacturer required
            print('## PTSENSOR MANUFACTURER REQUIRED IF PT SENSOR IS YES##')
            print(trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensormanufacturer.isnull())].tmp_row.tolist())
            trawl_args.update({
                "badrows": trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensormanufacturer.isnull())].tmp_row.tolist(),
                "badcolumn": 'PTSensorManufacturer',
                "error_type": "Undefined Error",
                "error_message" : 'PT Sensor Manufacturer required if PT Sensor is Yes'
            })
            warnings = [*warnings, checkData(**trawl_args)]


            ## Jordan - If PTSensor = Yes then PTSensorSerialNumber required. Added 9/18/18
            ## Check - If PTSensor = Yes then PTSensorSerialNumber required
            print('## PTSENSOR SERIALNUMBER REQUIRED IF PT SENSOR IS YES##')
            print(trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensorserialnumber.isnull())].tmp_row.tolist())
            trawl_args.update({
                "badrows": trawl[(trawl.ptsensor == 'Yes')&(trawl.ptsensorserialnumber.isnull())].tmp_row.tolist(),
                "badcolumn": 'PTSensorSerialNumber',
                "error_type": "Undefined Error",
                "error_message" : 'PT Sensor Serial Number is required if PT Sensor is Yes'
            })
            warnings = [*warnings, checkData(**trawl_args)]

            #Matthew M- Check that user has entered a comment if they selected a trawlfail code that requires comment. See lu_trawlfails, commentrequired field
            print("## Check that user has entered a comment if they selected a trawlfail code that requires comment. See lu_trawlfails, commentrequired field. ##")
            results= eng.execute("select lu_trawlfails.trawlfailure, lu_trawlfails.commentrequired from lu_trawlfails where UPPER(commentrequired) = 'YES';")
            lu_tf= pd.DataFrame(results.fetchall())
            lu_tf.columns=results.keys()
            lu_tf.columns = [x.lower() for x in lu_tf.columns]
            print(trawl[(trawl['trawlfail'].isin(lu_tf.trawlfailure.tolist()))& (trawl['comments'].isnull())])
            trawl_args.update({
                "badrows": trawl[(trawl['trawlfail'].isin(lu_tf.trawlfailure.tolist())) & (trawl['comments'].isnull())].tmp_row.tolist(),
                "badcolumn": 'Comments',
                "error_type": "Undefined Error",
                "error_message" : f'A comment is required for that trawlfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_trawlfails target=_blank>TrawlFail lookup</a>.'
            })
            errs = [*errs, checkData(**trawl_args)]

            # Check if trawl stations are in strata
            print("Check if trawl stations are in strata")
            # checker thing is breaking 
            

            if HAS_REGION_GEOMETRY:
                trawl_map_errors_df = check_strata_trawl(trawl, strata, field_assignment_table)

                #
                tmpstrata = strata.merge( trawl_map_errors_df[['stationid','region']], on = 'region', how = 'inner' )
                tmpstrata['SHAPE'] = tmpstrata['shape'] \
                    .apply(
                        lambda x: 
                        Geometry(
                            {
                                "spatialReference": {"wkid": 4326}, 
                                "rings": Geometry.from_shapely( wkb.loads(binascii.unhexlify(x)) ).rings 
                            }
                        )
                    )
                

                trawlpath = os.path.join(session['submission_dir'], "bad_trawl.json")
                bad_trawl_bight_region_path = os.path.join(session['submission_dir'], "bad_trawl_bight_regions.json")
                if len(trawl_map_errors_df) > 0:
                    export_sdf_to_json(trawlpath, trawl_map_errors_df)
                    export_sdf_to_json(
                        bad_trawl_bight_region_path , 
                        tmpstrata
                    )
                else:
                    if os.path.exists(trawlpath):
                        os.remove(trawlpath)
                    if os.path.exists(bad_trawl_bight_region_path):
                        os.remove(bad_trawl_bight_region_path)
                
                trawl_args.update({
                    "badrows": trawl_map_errors_df.tmp_row.tolist(),
                    "badcolumn": 'startlatitude,startlongitude, endlatitude, endlongitude',
                    "error_type": "Location Error",
                    "error_message" : f'This station has lat/longs outside of the region/stratum where the target lat/longs are (See Map tab for more details)'
                })
                warnings = [*warnings, checkData(**trawl_args)]


        else:
            # catch the case where the submission is only grab - trawl file should NOT be there...
            trawlpath = os.path.join(session['submission_dir'], "bad_trawl.json")
            bad_trawl_bight_region_path = os.path.join(session['submission_dir'], "bad_trawl_bight_regions.json")

            # This should also be removed if it exists, but there is a grab only file dropped
            badlinepath = os.path.join(session['submission_dir'], "bad_line_distances.json")

            if os.path.exists(trawlpath):
                os.remove(trawlpath)
            if os.path.exists(bad_trawl_bight_region_path):
                os.remove(bad_trawl_bight_region_path)
            if os.path.exists(badlinepath):
                os.remove(badlinepath)

        

        # ------- END Trawl Checks ------- #

        if grab is not None:
            # ------- Grab Checks ------- #
            print("Starting Grab Checks")
            ## jordan golemo - New calculated field (GrabDistanceToNominalTarget) . Look at Field Assignment Table target latitude/longitude. How are far off is Grab/Lat/Lon to target.
            print("##  New calculated field (GrabDistanceToNominalTarget) . Look at Field Assignment Table target latitude/longitude. How are far off is Grab/Lat/Lon to target ##")
            # create dataframe from Database field_assignment_table
            
            latlons = eng.execute('select distinct stationid, latitude AS targetlatitude, longitude AS targetlongitude, region from field_assignment_table;')
            db = pd.DataFrame(latlons.fetchall())
            db.columns = latlons.keys()
            
            # creates list of station_ids in the current database
            db_list = db.stationid.tolist()
            # dataframe of grab stationid / latitude / longitude
            grab_locs = pd.DataFrame({'stationid':grab['stationid'],'glat':grab['latitude'],'glon':grab['longitude']})
            # makes sure submitted stationids are found in database
            grab_locs['validstations'] = grab_locs.stationid.apply(lambda row: True if row in db_list else np.nan) 
            print("make sure submitted stationids are found in database")
            print(grab.loc[grab_locs.validstations.isnull()])
            grab_args.update({
                "badrows": grab.loc[grab_locs.validstations.isnull()].tmp_row.tolist(),
                "badcolumn": 'StationID',
                "error_type": "Undefined Error",
                "error_message" : 'Could not match submitted StationID to field assignment table'
            })
            errs = [*errs, checkData(**grab_args)]
            
            # matches grab lat/lon to target lat/lon by stationid 
            print("matches grab lat/lon to target lat/lon by stationid")
            coords = pd.merge(grab_locs,db, how = 'left', on='stationid')
            coords.dropna(inplace=True)
            # creates new field "DistanceToNominalTarget" for Grab
            #grab['grabdistancetonominaltarget'] = pd.Series([-88]*(len(grab)))
            coords['targetlatitude'] = coords['targetlatitude'].apply(lambda x: float(x))
            coords['targetlongitude'] = coords['targetlongitude'].apply(lambda x: float(x))
            grab['grabdistancetonominaltarget']=haversine_np(coords['glon'],coords['glat'],coords['targetlongitude'],coords['targetlatitude'])
            grab['grabdistancetonominaltarget'] = grab['grabdistancetonominaltarget'].replace(np.nan,-88)
            
            badgrabs = grab[
                grab.merge(db, how = 'left', on = 'stationid').apply(
                    lambda row:
                    (row['grabdistancetonominaltarget'] > 200) if 'channel islands' in str(row['region']).strip().lower() else (row['grabdistancetonominaltarget'] > 100),
                    axis = 1
                ) 
            ]

            print("badgrabs")
            print(badgrabs)
            
            if len(badgrabs) > 0:
                # append to bad_point distances
                bad_point_distances = pd.concat(
                    [
                        bad_point_distances, 
                        badgrabs[['stationid', 'grabdistancetonominaltarget']].assign(
                            error_message = "Grab Distance to Nominal Target > 100m (200m for channel islands)",
                            SHAPE = badgrabs.apply(lambda x: arcgisPoint({"x": x['longitude'], "y": x['latitude'], "spatialReference": {"wkid": 4326}}), axis=1)  
                        ).rename(columns = {'grabdistancetonominaltarget':'distance_to_target'})
                    ]
                )

                bad_point_distances.distance_to_target = bad_point_distances.distance_to_target.apply(
                    lambda x: np.round(x, 2)
                )

                grab_args.update({
                    "badrows":  badgrabs.tmp_row.tolist(),
                    "badcolumn": 'Latitude,Longitude',
                    "error_type": "Undefined Warning",
                    "error_message" : 'Grab Distance to Nominal Target > 100m (200m for channel islands)'
                })
                warnings = [*warnings, checkData(**grab_args)]


            # eric - check that Grab/Depth is more than 10% off of StationOccupation/Depth - warning only  - Will need to check database for StationOccupation whether user has provided or not. Same as trawl check.
            print("## Check that Grab/Depth is more than 10% off of StationOccupation/Depth - warning only  - Will need to check database for StationOccupation whether user has provided or not. Same as trawl check. ##")
            station_database = eng.execute('select distinct stationid from field_assignment_table;')
            db = pd.DataFrame(station_database.fetchall())
            db.columns = station_database.keys()
            # new code added by paul based on trawl above - bug not distinguishing grab and trawl
            occupation_grab = occupation[['stationid','occupationdepth','collectiontype']].where(occupation['collectiontype'].isin(['Grab']))
            print("occupation_grab")
            print(occupation_grab)
            # drop emptys
            occupation_grab = occupation_grab.dropna()
            # now we get the correct number of grabs
            # table_occupation merges the stationid from the database and the table(occupation) from submission
            table_occupation = pd.merge(occupation_grab[['stationid','occupationdepth']], db[['stationid']], how = 'left', on = 'stationid')
            # table_grab merges the stationid from the database and the table(grab) from (errors.xlxs)
            table_grab = pd.merge(grab[['stationid', 'stationwaterdepth','tmp_row']], db[['stationid']], how = 'left', on = 'stationid')
            # table_occ_grab merges the table occupation and table_grab based on their stationid
            table_occ_grab = pd.merge(table_occupation[['stationid','occupationdepth']], table_grab[['stationid', 'stationwaterdepth','tmp_row']], how= 'right', on = 'stationid')
            print(table_occ_grab.loc[(abs(table_occ_grab['stationwaterdepth'] - table_occ_grab['occupationdepth'])/table_occ_grab['stationwaterdepth']*100) > 10])
            grab_args.update({
                "badrows": table_occ_grab.loc[(abs(table_occ_grab['stationwaterdepth'] - table_occ_grab['occupationdepth'])/table_occ_grab['stationwaterdepth']*100) > 10].tmp_row.tolist(),
                "badcolumn": 'StationWaterDepth',
                "error_type": "Undefined Warning",
                "error_message" : 'Grab StationWaterDepth is more than 10 percent off Occupation Depth'
            })
            warnings = [*warnings, checkData(**grab_args)]



            # Matthew M - Check that user has entered a comment if they selected a grabfail code that requires comment. See lu_grabfails, commentrequired field.
            print("## Check that user has entered a comment if they selected a grabfail code that requires comment. See lu_grabfails, commentrequired field. ##")
            results = eng.execute("select lu_grabfails.grabfail, lu_grabfails.commentrequired from lu_grabfails where UPPER(commentrequired) = 'YES';")
            lu_gf= pd.DataFrame(results.fetchall())
            lu_gf.columns=results.keys()
            lu_gf.columns = [x.lower() for x in lu_gf.columns]
            print("lu_gf:")
            print(lu_gf)
            print("grab.comments")
            print(grab['comments'])
            print(grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist()))& (grab['comments'].isnull())])
            checkData(grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(), 'Comments', 'Undefined Error', 'error', f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.', grab)
            grab_args.update({
                "badrows": grab[(grab['grabfail'].isin(lu_gf.grabfail.tolist())) & (grab['comments'].isnull())].tmp_row.tolist(),
                "badcolumn": 'Comments',
                "error_type": "Undefined Error",
                "error_message" : f'A comment is required for that stationfail option. Please see: <a href=/{current_app.script_root}/scraper?action=help&layer=lu_grabfails target=_blank>GrabFail lookup</a>.'
            })
            errs = [*errs, checkData(**grab_args)]
            
            

            # Check for the parameters grabbed based on the sample assignment table
            all_param_names_dict = {
                'toxicity' : 'Toxicity', 
                'pfas': 'PFAS', 
                'pfasfieldblank': 'PFAS field blank', 
                'microplastics' : 'Microplastics', 
                'microplasticsfieldblank': 'Microplastics Field blank', 
                'grainsize' : 'Sediment Grain Size', 
                'sedimentchemistry': 'Sediment Chemistry', 
                'benthicinfauna' : 'Benthic Infauna'
            }

            # get a list of param names just based on the keys
            all_param_names = all_param_names_dict.keys()
            
            # Query sample assignment table along with xwalk to get the grab tables column names as parameter names
            assignments = pd.read_sql(f"SELECT s.stationid, x.grabtable_param_name AS parameter FROM sample_assignment_table s JOIN xwalk_sedgrab_parameter_names x on s.datatype = x.datatype", eng)

            # Mark just presence or absence of a parameter for a stationid
            assigned_params = assignments.pivot_table(index='stationid', columns='parameter', aggfunc=lambda x: 1, fill_value=0)

            # Reset the index
            assigned_params.reset_index(inplace=True)

            # Remove the column index name
            assigned_params.columns.name = None

            # add on the missing columns (if they are missing)
            for c in list( set(all_param_names) - set(assigned_params.columns) ):
                assigned_params[c] = 0

            def check_yes(grp):
                return 1 if (grp == 'Yes').any() else 0
            
            print('here')
            agg_dict = {
                param: check_yes
                for param in all_param_names
            }
            agg_dict.update({'tmp_row': list})
            has_params = grab.groupby('stationid').agg(agg_dict) 
            print("has params")
            print(has_params)
            has_params = has_params.reset_index()
            print("has params")
            print(has_params)
            print('yes')
            
            # merge them together for checking of presence and absence
            checkdf = has_params.merge(assigned_params, how = 'inner', on = 'stationid', suffixes = ('','_assigned'))

            if not checkdf.empty:
                for param, human_readable_param in all_param_names_dict.items():
                    badrows = checkdf[checkdf.apply(lambda row: (row[param] == 0) & (row[f"""{param}_assigned"""] == 1), axis = 1)].tmp_row.tolist()
                    badrows = [element for sublist in badrows for element in sublist]
                    
                    grab_args.update({
                        "badrows": badrows,
                        "badcolumn": 'stationid',
                        "error_type": "Assignment Error",
                        "error_message" : f'This station was assigned to have {human_readable_param} grabbed but it is recorded as missing from your grab table'
                    })
                    warnings = [*warnings, checkData(**grab_args)]


            if HAS_REGION_GEOMETRY:
                # Check if grab stations are in strata
                print("# Check if grab stations are in strata")
                # print("grab")
                # print(grab)
                # print("field_assignment_table")
                # print(field_assignment_table)
                bad_df = check_strata_grab(grab, strata, field_assignment_table)

                # export_sdf_to_json(bad_grab_region_path, strata[strata['region'].isin(bad_df['region'])].drop('shape', axis = 'columns', errors='ignore') )
                tmpstrata = strata[strata['region'].isin(bad_df['region'])]
                tmpstrata['SHAPE'] = tmpstrata['shape'] \
                    .apply(
                        lambda x: 
                        Geometry(
                            {
                                "spatialReference": {"wkid": 4326}, 
                                "rings": Geometry.from_shapely( wkb.loads(binascii.unhexlify(x)) ).rings 
                            }
                        )
                    )
                
                tmpstrata.drop('shape', axis = 'columns', errors='ignore')
                
                print("tmpstrata")
                print(tmpstrata)
                grabpath = os.path.join(session['submission_dir'], "bad_grab.json")
                bad_grab_region_path = os.path.join(session['submission_dir'], "bad_grab_bight_regions.json")
                if len(bad_df) > 0:
                    export_sdf_to_json(grabpath, bad_df.drop('shape', axis = 'columns', errors='ignore'))
                    export_sdf_to_json(
                        bad_grab_region_path, 
                        tmpstrata
                    )
                else:
                    if os.path.exists(grabpath):
                        os.remove(grabpath)
                    if os.path.exists(bad_grab_region_path):
                        os.remove(bad_grab_region_path)

                grab_args.update({
                    "badrows": bad_df.tmp_row.tolist(),
                    "badcolumn": 'latitude,longitude',
                    "error_type": "Location Error",
                    "error_message" : f'This station has lat/longs outside of the region/stratum where the target lat/longs are (See Map tab for more details)'
                })
                warnings = [*warnings, checkData(**grab_args)]
            print("end grab CHECKS")

            ## end grab CHECKS ##
        else:
            # catch the case where the submission is only trawl - grab file should NOT be there...
            grabpath = os.path.join(session['submission_dir'], "bad_grab.json")
            bad_grab_region_path = os.path.join(session['submission_dir'], "bad_grab_bight_regions.json")

            if os.path.exists(grabpath):
                os.remove(grabpath)
            if os.path.exists(bad_grab_region_path):
                os.remove(bad_grab_region_path)



        # export bad line distances geojson
        # Also export the geojson to report the polyline being too far from the target
        badlinepath = os.path.join(session['submission_dir'], "bad_line_distances.json")
        if len(bad_line_distances) > 0:
            export_sdf_to_json(
                badlinepath, bad_line_distances
            )
        else:
            if os.path.exists(badlinepath):
                os.remove(badlinepath)
        
        # Somehow we need to distinguish bad grabs from bad occupations.... i didnt think of that....
        # export bad point distances geojson
        badpointpath = os.path.join(session['submission_dir'], "bad_point_distances.json")
        if len(bad_point_distances) > 0:
            export_sdf_to_json(
                badpointpath, bad_point_distances
            )
        else:
            if os.path.exists(badpointpath):
                os.remove(badpointpath)


        # export geojson with target latlongs
        targets = occupation[['stationid']].merge(field_assignment_table[['stationid','latitude','longitude']], on = 'stationid', how = 'inner') \
            .drop_duplicates()

        targets['SHAPE'] = targets.apply(
            lambda row: arcgisPoint({                
                "x" :  row['longitude'], 
                "y" :  row['latitude'], 
                "spatialReference" : {'latestWkid': 4326, 'wkid': 4326}
            }),
            axis=1
        )

        targets = targets.merge(field_assignment_table[['stationid','stratum','region']].drop_duplicates(), on = 'stationid', how = 'left')
        
        targetpath = os.path.join(session['submission_dir'], "target_stations.json")
        if os.path.exists(targetpath):
            os.remove(targetpath)
        export_sdf_to_json(targetpath, targets)

    return {'errors': errs, 'warnings': warnings}
