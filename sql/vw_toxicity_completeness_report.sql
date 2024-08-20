CREATE OR REPLACE VIEW vw_toxicity_completeness_report AS 
SELECT
CASE
		
	WHEN
		( ( COALESCE ( sample_assignment_table.submissionstatus, '' :: CHARACTER VARYING ) ) :: TEXT = '' :: TEXT ) THEN
			'missing' :: CHARACTER VARYING ELSE sample_assignment_table.submissionstatus 
			END AS submissionstatus,
		sample_assignment_table.lab,
		sample_assignment_table.PARAMETER,
		string_agg ( ( sample_assignment_table.stationid ) :: TEXT, ', ' :: TEXT ) AS stations 
	FROM
		sample_assignment_table 
	WHERE
		(
			( ( sample_assignment_table.stationid ) :: TEXT ~~ 'B18%' :: TEXT ) 
			AND ( ( sample_assignment_table.datatype ) :: TEXT = 'Toxicity' :: TEXT ) 
		) 
	GROUP BY
		sample_assignment_table.submissionstatus,
		sample_assignment_table.lab,
		sample_assignment_table.PARAMETER 
	ORDER BY
	CASE
			
			WHEN ( ( COALESCE ( sample_assignment_table.submissionstatus, '' :: CHARACTER VARYING ) ) :: TEXT = '' :: TEXT ) THEN
			'missing' :: CHARACTER VARYING ELSE sample_assignment_table.submissionstatus 
		END DESC,
	sample_assignment_table.lab,
	sample_assignment_table.PARAMETER;