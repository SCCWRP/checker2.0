from json import dump
from pandas import DataFrame

# a function used to collect the warnings to store in database
# in other words, if a user submitted flagged data we want to store that information somewhere in the database to make it easier to hunt down flagged data
def collect_error_messages(errs):
    """
    errs is the list of errors or warnings that were collected during the checking process
    offset is the number of rows the excel files are offsetted, based on how the people set up their data submission templates
    """

    assert isinstance(errs, list), "function - collect_warnings - errs object is not a list"
    if errs == []:
        return []
    assert all([isinstance(x, dict) for x in errs]), "function - collect_warnings - errs list contains non-dictionary objects"
    assert all(["columns" in x.keys() for x in errs]), "function - collect_warnings - 'columns' not found in keys of a dictionary in the errs list"
    assert all(["rows" in x.keys() for x in errs]), "function - collect_warnings - 'rows' not found in keys of a dictionary in the errs list"
    assert all(["table" in x.keys() for x in errs]), "function - collect_warnings - 'table' not found in keys of a dictionary in the errs list"
    assert all(["error_message" in x.keys() for x in errs]), "function - collect_warnings - 'error_message' not found in keys of a dictionary in the errs list"


    output = [
        {
            # This will be written to a json and stored in the submission directory
            # to be read in later during the final submission routine, 
            # or in the routine which marks up their excel file
            "columns"         : w['columns'],
            "table"           : w['table'],
            "row_number"      : r['row_number'],
            "message"         : f"{w['columns']} - {w['error_message']}"
        }
        for w in errs
        for r in w['rows']
    ]

    output = DataFrame(output).groupby(['row_number', 'columns', 'table']) \
        .apply(
            # .tolist() doesnt work. 
            lambda x: ';'.join( list(x['message']) ) 
        ).to_dict() 

    
    return [{'row_number': k[0], 'columns': k[1], 'table': k[2], 'message': v} for k, v in output.items()]




def save_errors(errs, filepath):
    errors_file = open(filepath, 'w')
    dump(
        collect_error_messages(errs),
        errors_file
    )
    errors_file.close()