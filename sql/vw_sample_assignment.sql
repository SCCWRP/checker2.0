CREATE OR REPLACE VIEW vw_sample_assignment AS 
SELECT
	sample_assignment_table.objectid,
	sample_assignment_table.stationid,
	field_assignment_feature.targetlatitude,
	field_assignment_feature.targetlongitude,
	sample_assignment_table.lab,
	sample_assignment_table.PARAMETER,
	sample_assignment_table.stationstatus,
	sample_assignment_table.submissionstatus,
	sample_assignment_table.datatype,
	sample_assignment_table.submissiondate,
	field_assignment_feature.shape 
FROM
	( field_assignment_feature JOIN sample_assignment_table ON ( ( ( field_assignment_feature.stationid ) :: TEXT = ( sample_assignment_table.stationid ) :: TEXT ) ) ) 
WHERE
	( ( sample_assignment_table.stationid ) :: TEXT ~~ 'B18%' :: TEXT ) 
ORDER BY
	sample_assignment_table.stationid,
	sample_assignment_table.datatype,
	sample_assignment_table.lab,
	sample_assignment_table.PARAMETER;