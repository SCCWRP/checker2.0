import re
from pandas import read_sql, Timestamp, isnull


def check_dtype(t, x):
    try:
        t(x)
        return True
    except Exception as e:
        return False

class GeoDBDataFrame(pd.DataFrame):
    def __init__(self, eng, *args, **kwargs):
        super(GeoDBDataFrame, self).__init__(*args, **kwargs)
        self.eng = eng

    @property
    def _constructor(self):
        return(GeoDBDataFrame)

    def to_geodb(self, tablename):
        tbl_cols = read_sql(f"SELECT * FROM information_schema.columns WHERE table_name = '{tablename}';", self.eng) \
            .column_name \
            .tolist()
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
                                    if ( (check_dtype(float, val)) or (check_dtype(int, val)) or ("sde.next_" in str(val)) )

                                    # If all else fails its basically either a character or a time
                                    # in which case we wrap in single quotes
                                    # single quotes within a string are escaped by doubling them
                                    # not by using a backslash
                                    else "'{}'".format(str(val).replace("'","''"))  
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

            self.eng.execute(finalsql)
        else:
            print("Nothing to load.")





def fetch_meta(tablename, eng):

    meta = read_sql(
            f"""
            SELECT 
                table_name, 
                column_name, 
                is_nullable, 
                data_type,
                udt_name, 
                character_maximum_length, 
                numeric_precision, 
                numeric_scale 
            FROM 
                information_schema.columns 
            WHERE 
                table_name = '{tablename}';
            """, 
            eng
        )

    meta['dtype'] = meta \
        .udt_name \
        .apply(
            # This pretty much only works if the columns were defined through Arc
            lambda x: 
            int if 'int' in x 
            else str if x == 'varchar' 
            else Timestamp if x == 'timestamp' 
            else float if x == 'numeric' 
            else None
        )  

    return meta



# This function allows you to put in a table name and get back the primary key fields of the table
def get_primary_key(tablename, eng):
    # eng is a sqlalchemy database connection

    # This query gets us the primary keys of a table. Not in a python friendly format
    # Copy paste to Navicat, pgadmin, or do a read_sql to see what it gives
    pkey_query = f"""
        SELECT 
            conrelid::regclass AS table_from, 
            conname, 
            pg_get_constraintdef(oid) 
        FROM pg_constraint 
        WHERE 
            contype IN ('f', 'p') 
            AND connamespace = 'sde'::regnamespace 
            AND conname LIKE '{tablename}%%' 
        ORDER BY 
            conrelid::regclass::text, contype DESC;
    """
    pkey_df = read_sql(pkey_query, eng)
    
    pkey = []
    # sometimes there is no primary key
    if not pkey_df.empty:
        # pg_get_constraintdef = postgres get constraint definition
        # Get the primary key constraint's definition
        pkey = pkey_df.pg_get_constraintdef.tolist()[0]

        # Remove excess junk to just get the primary key field names
        # split at the commas to get a nice neat python list
        pkey = re.sub(r"(PRIMARY\sKEY\s\()|(\))","",pkey).split(',')

        # remove whitespace from the edges
        pkey = [colname.strip() for colname in pkey]
        
    return pkey