import pandas as pd
from flask import g

def get_login_field(table, displayfield, valuefield, **kwargs):
    # dtypes is a dictionary that needs to be configured a certain way
    # login_fields = [dt.get('fieldname') for dt in dtypes.get(dtype).get('login_fields')]
    
    # assert set(kwargs.keys()).issubset(set(login_fields)), \
    #     f"The keyword args {', '.join(kwargs.keys())} not found in the valid login fields: {', '.join(login_fields)}"

    eng = g.eng

    if (len(kwargs) == 0):
        sql = f"""SELECT DISTINCT {displayfield} AS displayvalue, {valuefield} AS actualvalue FROM {table} ORDER BY 1;"""
    else:
        print("kwargs")
        print(kwargs)
        assert \
            all(
                k in pd.read_sql(f"""SELECT DISTINCT column_name FROM information_schema.columns WHERE table_name = '{table}';""", eng).column_name.values
                for k in kwargs.keys()
            ), \
            f"Not all kwargs keys found in the columns of {table}"

        sql = f"""
            SELECT DISTINCT {displayfield} AS displayvalue, {valuefield} AS actualvalue FROM {table} 
            WHERE 
            {
                ' AND '.join([f"{k} = '{v}'" for k,v in kwargs.items() if k not in ('field','dtype')])
            }
            ORDER BY 1;
        """
    print(sql)
    # object of type ndarray is not json serializable, so we have to return a list rather than a numpy array
    vals = pd.read_sql(sql, eng).to_dict('records')
    return vals