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

            eng.execute(finalsql)
        else:
            print("Nothing to load.")


