from json import dump
from pandas import DataFrame
from inspect import currentframe


# a function used to collect the warnings to store in database
# in other words, if a user submitted flagged data we want to store that information somewhere in the database to make it easier to hunt down flagged data
def collect_error_messages(errs):
    """
    errs is the list of errors or warnings that were collected during the checking process
    offset is the number of rows the excel files are offsetted, based on how the people set up their data submission templates
    """

    assert isinstance(errs, list), "function - collect_warnings - errs object is not a list"
    errs = [e for e in errs if len(e) > 0]
    if errs == []:
        return []
    assert all([isinstance(x, dict) for x in errs]), "function - collect_warnings - errs list contains non-dictionary objects"
    
    
    for k in ('columns','rows','table','error_message'):
        assert all([k in x.keys() for x in errs]), f"function - collect_warnings - '{k}' not found in keys of a dictionary in the errs list"
    
    print('in collect error messages')
    print("errs")
    output = [
        {
            # This will be written to a json and stored in the submission directory
            # to be read in later during the final submission routine, 
            # or in the routine which marks up their excel file
            # If i really wanted to do it officially, i'd probably make the message a json format
            # I decided not to do it this way because i can imagine a lot of bugs happening, such as quotes being in error messages, 
            #   colons in error messages, etc
            # Instead i separate the columns and the associated error message with 3 hyphens
            # As long as 3 consecutive hyphens with spaces on both sides doesnt show up in an error message, it will work
            # I dont like doing it this way, but i'm thinking it might be the lesser of two evils, 
            #   since doing it with json we will have to test every possible case that could break it
            #   So this way is not elegant, but less likely to break with unexpected input
            "columns"         : e['columns'],
            "table"           : e['table'],
            "row_number"      : r,
            "message"         : f"{e['columns']} --- {e['error_message']}"
        }
        for e in errs
        for r in e['rows']
    ]

    # print("output from generic.py before groupby: ")
    # print(output)

    output = DataFrame(output).groupby(['row_number', 'table']) \
        .apply(
            # .tolist() doesnt work. 
            lambda x: '; '.join( list(x['message']) ) 
        ).to_dict() 

    # print("output from generic.py after groupby: ")
    # print(output)
    
    return [{'row_number': k[0], 'table': k[1], 'message': v} for k, v in output.items()]



def correct_row_offset(lst, offset):
    # By default the error and warnings collection methods assume that no rows were skipped in reading in of excel file.
    # It adds 1 to the row number when getting the error/warning, since excel is 1 based but the python dataframe indexing is zero based.
    # Therefore the row number in the errors and warnings will only match with their excel file's row if the column headers are actually in 
    #   the first row of the excel file.
    # These next few lines of code should correct that
    #print("lst: ")
    #print(lst)
    print("offset: ")
    print(offset)

    [
        e.update(
            { "rows" : [ r + offset + 1 + 1 for r in e['rows']] }
        )
        for e in lst
        if len(e) > 0
        
    ]

    return lst



def save_errors(errs, filepath):
    errors_file = open(filepath, 'w')
    dump(
        collect_error_messages(errs),
        errors_file
    )
    errors_file.close()


