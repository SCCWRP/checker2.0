import psycopg2
from psycopg2 import sql
from pandas import read_sql, isnull, DataFrame


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
            kcu.table_name,
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


def foreign_key_detail(table, eng):
    '''
    table is the tablename you want the foreign key relationships for
    eng is the database connection
    '''

    sql = f"""
        SELECT 
            src_table.relname AS table_name,
            src_col.attname AS column_name,
            tgt_table.relname AS foreign_table_name,
            tgt_col.attname AS foreign_column_name
        FROM 
            pg_constraint con
            JOIN pg_class src_table ON con.conrelid = src_table.oid
            JOIN pg_class tgt_table ON con.confrelid = tgt_table.oid
            JOIN unnest(con.conkey) WITH ORDINALITY AS src_col_nums(src_attnum, ord)
                ON true
            JOIN unnest(con.confkey) WITH ORDINALITY AS tgt_col_nums(tgt_attnum, ord)
                ON src_col_nums.ord = tgt_col_nums.ord
            JOIN pg_attribute src_col ON src_col.attrelid = src_table.oid AND src_col.attnum = src_col_nums.src_attnum
            JOIN pg_attribute tgt_col ON tgt_col.attrelid = tgt_table.oid AND tgt_col.attnum = tgt_col_nums.tgt_attnum
        WHERE 
            con.contype = 'f'
            AND src_table.relname = '{table}';
    """

    dat = read_sql(sql, eng)
    if dat.empty:
        return {}

    # Build the dictionary in the desired structure
    result = {}
    
    for _, row in dat.iterrows():
        table_name = row['table_name']
        column_name = row['column_name']
        foreign_table_name = row['foreign_table_name']
        foreign_column_name = row['foreign_column_name']
        
        if table_name not in result:
            result[table_name] = {}
        
        result[table_name][column_name] = {
            "referenced_table": foreign_table_name,
            "referenced_column": foreign_column_name
        }

    return result


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


# Get the column comments for the excel data submission template
def get_column_comments(table, eng):
    query = f"""
        SELECT
            cols.TABLE_NAME AS tablename,
            cols.COLUMN_NAME AS column_name,
            (
                SELECT
                    pg_catalog.col_description ( C.oid, cols.ordinal_position :: INT ) 
                FROM
                    pg_catalog.pg_class C 
                WHERE
                    C.oid = ( SELECT ( '"' || cols.TABLE_NAME || '"' ) :: regclass :: oid ) 
                    AND C.relname = cols.TABLE_NAME 
            ) AS column_comment 
        FROM
            information_schema.COLUMNS cols 
        WHERE
            cols.TABLE_NAME = '{table}'
    """
    return read_sql(query, eng)


# fix the column order
# this function is designed to fix problems with the templater and query routine which cause the thing to break when a column name changes
# rather than repeating this code block in 3 different places where it needs to run, we are making this function
def update_column_order_table(DB_HOST, DB_NAME, DB_USER, PGPASSWORD, column_order_table = 'column_order'):

    # connect with psycopg2
    connection = psycopg2.connect(
        host = DB_HOST,
        database = DB_NAME,
        user = DB_USER,
        password = PGPASSWORD
    )

    connection.set_session(autocommit=True)

    # update column-order table based on contents of information schema
    cols_to_add_qry = sql.SQL(
            """
            WITH cols_to_add AS (
                SELECT 
                    table_name,
                    column_name,
                    ordinal_position AS original_db_position,
                    ordinal_position AS custom_column_position 
                FROM
                    information_schema.COLUMNS 
                WHERE
                    table_name IN ( SELECT DISTINCT table_name FROM {column_order_table} ) 
                    AND ( table_name, column_name ) NOT IN ( SELECT DISTINCT table_name, column_name FROM {column_order_table} )
            )
            INSERT INTO 
                {column_order_table} (table_name, column_name, original_db_position, custom_column_position) 
                (
                    SELECT table_name, column_name, original_db_position, custom_column_position FROM cols_to_add
                )
            ;
            """
        ).format(
            column_order_table = sql.Identifier(column_order_table),
        )

    # remove records from column order if they are not there anymore
    cols_to_delete_qry = sql.SQL(
            """
            WITH cols_to_delete AS (
                SELECT TABLE_NAME
                    ,
                    COLUMN_NAME,
                    original_db_position,
                    custom_column_position 
                FROM
                    {column_order_table} 
                WHERE
                    TABLE_NAME NOT IN ( SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS ) 
                    OR ( TABLE_NAME, COLUMN_NAME ) NOT IN ( SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS ) 
                ) 
                DELETE FROM {column_order_table} 
                WHERE
                    ( TABLE_NAME, COLUMN_NAME ) IN ( SELECT TABLE_NAME, COLUMN_NAME FROM cols_to_delete )
            ;
            """
        ).format(
            column_order_table = sql.Identifier(column_order_table)
        )
    

    with connection.cursor() as cursor:
        cursor.execute(cols_to_add_qry)
        cursor.execute(cols_to_delete_qry)
        
    connection.close()

    return None