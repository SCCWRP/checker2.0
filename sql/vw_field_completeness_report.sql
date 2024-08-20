CREATE OR REPLACE VIEW vw_field_completeness_report AS 
SELECT
CASE
		
	WHEN
		( ( COALESCE ( tmp.submissionstatus, '' :: CHARACTER VARYING ) ) :: TEXT = '' :: TEXT ) THEN
			'missing' :: CHARACTER VARYING ELSE tmp.submissionstatus 
			END AS submissionstatus,
		tmp.lab,
		tmp.TYPE AS PARAMETER,
		string_agg ( ( tmp.stationid ) :: TEXT, ', ' :: TEXT ) AS stations 
	FROM
		(
		SELECT
			field_assignment_table.grabagency AS lab,
			field_assignment_table.grabsubmit AS submissionstatus,
			field_assignment_table.stationid,
			'grab' :: TEXT AS TYPE 
		FROM
			field_assignment_table 
		WHERE
			( ( field_assignment_table.grab ) :: TEXT = 'Yes' :: TEXT ) UNION ALL
		SELECT
			field_assignment_table.trawlagency AS lab,
			field_assignment_table.trawlsubmit AS submissionstatus,
			field_assignment_table.stationid,
			'trawl' :: TEXT AS TYPE 
		FROM
			field_assignment_table 
		WHERE
			( ( field_assignment_table.trawl ) :: TEXT = 'Yes' :: TEXT ) UNION ALL
		SELECT
			field_assignment_table.grabagency AS lab,
			'abandoned' :: CHARACTER VARYING AS submissionstatus,
			field_assignment_table.stationid,
			'grab' :: TEXT AS TYPE 
		FROM
			field_assignment_table 
		WHERE
			(
				( ( field_assignment_table.grababandoned ) :: TEXT = 'Yes' :: TEXT ) 
				AND ( ( field_assignment_table.grab ) :: TEXT = 'Yes' :: TEXT ) 
			) UNION ALL
		SELECT
			field_assignment_table.trawlagency AS lab,
			'abandoned' :: CHARACTER VARYING AS submissionstatus,
			field_assignment_table.stationid,
			'trawl' :: TEXT AS TYPE 
		FROM
			field_assignment_table 
		WHERE
			(
				( ( field_assignment_table.trawlabandoned ) :: TEXT = 'Yes' :: TEXT ) 
				AND ( ( field_assignment_table.trawl ) :: TEXT = 'Yes' :: TEXT ) 
			) 
		) tmp 
	WHERE
		( ( tmp.stationid ) :: TEXT ~~ 'B18%' :: TEXT ) 
	GROUP BY
		tmp.submissionstatus,
		tmp.lab,
		tmp.TYPE 
	ORDER BY
	CASE
			
			WHEN ( ( COALESCE ( tmp.submissionstatus, '' :: CHARACTER VARYING ) ) :: TEXT = '' :: TEXT ) THEN
			'missing' :: CHARACTER VARYING ELSE tmp.submissionstatus 
		END DESC,
	tmp.lab,
	tmp.TYPE;