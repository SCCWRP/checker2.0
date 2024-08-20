from inspect import currentframe
from flask import current_app, g
from .field import fieldchecks

def field_trawl_and_grab(all_dfs):
    
    current_function_name = str(currentframe().f_code.co_name)
    print("current_function_name")
    print(current_function_name)
    
    # function should be named after the dataset in app.datasets in __init__.py
    assert current_function_name in current_app.datasets.keys(), \
        f"function {current_function_name} not found in current_app.datasets.keys() - naming convention not followed"

    expectedtables = set(current_app.datasets.get(current_function_name).get('tables'))
    assert expectedtables.issubset(set(all_dfs.keys())), \
        f"""In function {current_function_name} - {expectedtables - set(all_dfs.keys())} not found in keys of all_dfs ({','.join(all_dfs.keys())})"""
        
    eng = g.eng

    occupation = all_dfs['tbl_stationoccupation']
    trawl = all_dfs['tbl_trawlevent']
    grab = all_dfs['tbl_grabevent']
    
    occupation['tmp_row'] = occupation.index
    trawl['tmp_row'] = trawl.index
    grab['tmp_row'] = grab.index
    
    # fieldchecks function returns the dictionary of errors and warnings
    return fieldchecks(occupation, eng, trawl = trawl, grab = grab)
