import re
from pandas import read_sql, Timestamp, isnull, DataFrame


def check_dtype(t, x):
    try:
        t(x)
        return True
    except Exception as e:
        return False

class GeoDBDataFrame(DataFrame):
    def __init__(self, *args, **kwargs):
        super(GeoDBDataFrame, self).__init__(*args, **kwargs)

    @property
    def _constructor(self):
        return(GeoDBDataFrame)

    def to_geodb(self, tablename, eng):
        tbl_cols = read_sql(f"SELECT * FROM information_schema.columns WHERE table_name = '{tablename}';", eng) \
            .column_name \
            .tolist()

        # For our checker application we should definitely enforce all columns being the same.
        # Thus we will make these assert statements for faster troubleshooting and debugging.
        assert set(self.columns) - set(tbl_cols) == set(), \
            f"Dataframe has columns not found in table {tablename}: {','.join(set(self.columns) - set(tbl_cols))}"
        

        if not self.empty:
            # this used to have ON CONFLICT ON CONSTRAINT (prinary key) DO NOTHING
            # but that was in the bmpsync routine. I'm not sure if we want to include that here.

            # TODO assert that the columns match up between the dataframe and table

            finalsql = """
                INSERT INTO {} \n({}) \nVALUES {}
                """ \
                .format(
                    tablename, 
                    ', '.join(set(self.columns).intersection(set(tbl_cols))),
                    ',\n'.join(
                        "({})" \
                        .format(
                            ', '.join(
                                [
                                    'NULL'
                                    if ( (str(val).strip() == '') or (isnull(val)) )
                                    
                                    # checks if the string literal is numeric or not
                                    # If it is, we do not want to wrap it in single quotes
                                    # If its an arc function, we also dont want to wrap it in quotes in that case
                                    else str(val).strip()
                                    #if ( (check_dtype(float, val)) or (check_dtype(int, val)) or ("sde.next_" in str(val)) )
                                    if ( ("sde.next_" in str(val)) )

                                    # If all else fails its basically either a character or a time
                                    # in which case we wrap in single quotes
                                    # single quotes within a string are escaped by doubling them
                                    # not by using a backslash
                                    else "'{}'".format(str(val).strip().replace("'","''"))  
                                    for val in x
                                ]
                            )
                        )
                        for x in 
                        list(zip(*[self[c] for c in set(self.columns).intersection(set(tbl_cols))]))
                    ),
                    tablename
                ) \
                .replace("%","%%")

            eng.execute(finalsql)
        else:
            print("Nothing to load.")



# Get the registration id from the geodatabase
def registration_id(tablename, conn):
    reg_ids = read_sql(f"SELECT registration_id, table_name FROM sde.sde_table_registry WHERE table_name = '{tablename}';", conn).registration_id.values
    
    assert len(reg_ids) > 0, f"Registration ID for table {tablename} not found - table may not be registered with the geodatabase!"
    
    return reg_ids[0]

# Get what the next object ID would be for the table
def next_objectid(tablename, conn):
    reg_id = registration_id(tablename, conn)
    if reg_id:
        if not read_sql(f"SELECT * FROM information_schema.tables WHERE table_name = 'i{reg_id}'", conn).empty:
            return read_sql(f"SELECT base_id FROM i{reg_id}", conn).base_id.values[0]
        else:
            raise Exception(f'Table i{reg_id} not found (the table is supposed to correspond to {tablename})')
    else:
        raise Exception(f'No registration ID found for table {tablename}')

# Get primary key of a table as a list of column names
def primary_key(table, eng):
    '''
    table is the tablename you want the primary key for
    eng is the database connection
    '''

    sql = f'''
        SELECT
            tc.TABLE_NAME,
            C.COLUMN_NAME,
            C.data_type 
        FROM
            information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu USING ( CONSTRAINT_SCHEMA, CONSTRAINT_NAME )
            JOIN information_schema.COLUMNS AS C ON C.table_schema = tc.CONSTRAINT_SCHEMA 
            AND tc.TABLE_NAME = C.TABLE_NAME 
            AND ccu.COLUMN_NAME = C.COLUMN_NAME 
        WHERE
            constraint_type = 'PRIMARY KEY' 
            AND tc.TABLE_NAME = '{table}';
    '''

    return read_sql(sql, eng).column_name.tolist()

def foreign_keys(table, eng):
    '''
    table is the tablename you want the foreign key relationships for
    eng is the database connection
    '''

    sql = f"""
        SELECT
        DISTINCT
            kcu.column_name, 
            ccu.table_name AS foreign_table_name
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' 
        AND tc.table_name='{table}'
        AND ccu.table_name LIKE 'lu_%%';
    """

    dat = read_sql(sql, eng)
    return dat.set_index('column_name')['foreign_table_name'].to_dict() if not dat.empty else dict()



# In the part that gets the column comments we might need also :
#   WHERE table_catalog = {os.environ.get('DB_NAME')}
def metadata_summary(table, eng):
    sql = f"""
    WITH fkeys AS (
        SELECT DISTINCT
            kcu.COLUMN_NAME,
            ccu.TABLE_NAME AS foreign_table_name 
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME 
            AND ccu.table_schema = tc.table_schema 
        WHERE
            tc.constraint_type = 'FOREIGN KEY' 
            AND tc.TABLE_NAME = '{table}' 
            AND ccu.TABLE_NAME LIKE'lu_%%' 
	),
	pkey AS (
        SELECT C
            .COLUMN_NAME,
            'YES' AS primary_key 
        FROM
            information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage AS ccu USING ( CONSTRAINT_SCHEMA, CONSTRAINT_NAME )
            JOIN information_schema.COLUMNS AS C ON C.table_schema = tc.CONSTRAINT_SCHEMA 
            AND tc.TABLE_NAME = C.TABLE_NAME 
            AND ccu.COLUMN_NAME = C.COLUMN_NAME 
        WHERE
            constraint_type = 'PRIMARY KEY' 
		AND tc.table_name = '{table}' 
	),
	cmt AS (
        SELECT
            cols.table_name AS tablename,
            cols.COLUMN_NAME AS COLUMN_NAME,
            (
            SELECT
                pg_catalog.col_description ( C.oid, cols.ordinal_position :: INT ) 
            FROM
                pg_catalog.pg_class C 
            WHERE
                C.oid = ( SELECT ( '"' || cols.table_name || '"' ) :: regclass :: oid ) 
                AND C.relname = cols.table_name 
            ) AS description 
        FROM
            information_schema.COLUMNS cols 
        WHERE 
            cols.table_name = '{table}' 
	) ,
	colorder AS (
		SELECT table_name AS tablename, column_name, custom_column_position AS column_position FROM column_order WHERE table_name = '{table}'
	)
    SELECT
        isc.table_name AS tablename,
        isc.COLUMN_NAME,
        isc.udt_name AS datatype,
        CASE WHEN isc.is_nullable = 'NO' THEN 'YES' ELSE' NO' END AS required,
        isc.character_maximum_length AS character_limit,
        pkey.primary_key,
        fkeys.foreign_table_name AS lookuplist_table_name,
        cmt.description 
    FROM information_schema.COLUMNS isc
        LEFT JOIN pkey ON isc.column_name = pkey.column_name 
        LEFT JOIN fkeys ON fkeys.column_name = isc.column_name 
        LEFT JOIN cmt ON isc.table_name = cmt.tablename AND isc.column_name = cmt.column_name 
        LEFT JOIN colorder ON isc.table_name = colorder.tablename AND isc.column_name = colorder.column_name 
    WHERE
        TABLE_NAME = '{table}'
				ORDER BY colorder.column_position;
    """
    return read_sql(sql, eng)