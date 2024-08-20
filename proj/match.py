import pandas as pd
from copy import deepcopy
from flask import session, current_app, g
from gc import collect
from openpyxl import load_workbook


def match(all_dfs):

    eng = g.eng
    system_fields = current_app.system_fields
    datasets = current_app.datasets

    # Key value pairs, keys being the table, values being the excel tab name the user gave us
    # To be used in the error reporting system on the front end when the app returns the json response to the browser
    table_to_tab_map = dict()

    match_tbls_sql = f"""
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE ((table_name LIKE 'tbl_%%') OR (table_name = '{current_app.config.get("TOXSUMMARY_TABLENAME")}'))
        AND column_name NOT IN ('{"','".join(system_fields)}')
        AND column_name NOT LIKE 'login_%%'
        ;"""

    cols_df = pd.read_sql(match_tbls_sql, eng) \
        .groupby('table_name') \
        .apply(lambda x: x.column_name.tolist() ) \
        .reset_index(name='colnames')
    
    # if someone would want to see what the above dataframe looks like, you can uncomment this line
    # but it should result in a 2 column dataframe, with the table name in one column, and all the column names
    # of that table in the other column, separated by pipes
    # print("cols_df")
    # print(cols_df)

    # we assume that query will return results
    assert not cols_df.empty, "match.py - dataframe for which tables have which columns, came up empty for some reason"

    match_report = []

    # unless we make a deep copy of all_dfs, it will alter all_dfs during the for loop
    # but if we iterate on the items of all_dfs and all_dfs changes during the steps of iteration, thats a big problem
    # I am surprised there wasnt an infinite loop before
    # We can delete tmp_dfs after and collect the garbage
    tmp_dfs = deepcopy(all_dfs)
    
    matched_tables = []

    for sheetname, df in tmp_dfs.items():

        # joining the submitted dataframe by pipes and trying to match

        m = cols_df[
            # each item in the colnames column should be a set
            cols_df.colnames.apply(lambda x: set(x) == set(df.columns))
        ]

        print("df.columns")
        print(df.columns)
        

        if m.empty:
            print(f"No match for {sheetname} - finding closest match")
            cols_df['in_tab_not_table'] = cols_df.colnames.apply(
                lambda x: list( set(df.columns.tolist()) - set(x) )
            )
            cols_df['in_table_not_tab'] = cols_df.colnames.apply(
                lambda x:  list( set(x) - set(df.columns.tolist()) )
            )

            # symdiff = symmetric difference
            cols_df['symdiff_len'] = cols_df.colnames.apply(lambda x:  len(set(x).symmetric_difference(set(df.columns.tolist()))) )


            closest = cols_df[cols_df.symdiff_len == cols_df.symdiff_len.min()]
            
            # We assume its not empty
            assert not closest.empty, "match.py - dataframe with variable name closest is empty for some reason"

            # iloc zero (grab only first row) in case mutliple tables tied for being the closest
            closest = closest.iloc[0].to_dict()

            match_report.append(
                {
                    "sheetname"        : sheetname,
                    "tablename"        : "", 
                    "in_tab_not_table" : ', '.join(closest["in_tab_not_table"]),
                    "in_table_not_tab" : ', '.join(closest["in_table_not_tab"]), 
                    "closest_tbl"      : closest["table_name"] 
                }
            )

        else:
            print(f"found match for {sheetname}")
            # m should only have one row based on our assumption covered in the assert statement above the for loop
            matched_tbl = m.iloc[0, m.columns.get_loc("table_name")]

            # append it to the matched_tables list
            matched_tables.append(matched_tbl)

            # assign the dataframe with a key being the name of the table it matched in the db
            all_dfs[matched_tbl] = all_dfs.pop(sheetname)

            table_to_tab_map[matched_tbl] = sheetname

            # Rename the worksheet in the original excel file
            # https://stackoverflow.com/questions/39540789/how-to-rename-the-sheet-name-in-the-spread-sheet-using-python
            # ss stands for spreadsheet
            # ss = load_workbook(session['excel_path'])
            # ss_sheet = ss[sheetname]
            # ss_sheet.title = matched_tbl
            # ss.save(session['excel_path'])

            match_report.append(
                {
                    "sheetname"        : sheetname,
                    "tablename"        : matched_tbl, 
                    "in_tab_not_table" : "", 
                    "in_table_not_tab" : "", 
                    "closest_tbl"      : "" 
                }
            )
        
    # delete the temp object and collect garbage to save memory
    del tmp_dfs
    collect()

    
    
    # I am not quite sure why we now do it so that the tables list matches the matched_tables list rather than the keys of the dictionary
    # But we will use this to get rid of the tox summary tab if they are submitting tox data with a summary
    # match_dataset = [k for k,v in datasets.items() if set(v.get('tables')) == set(all_dfs.keys())]
    # match_dataset = [k for k,v in datasets.items() if set(v.get('tables')) == set(matched_tables)]

    # Once again, we are changing the way match_dataset gets defined
    # The logic that previously defined match_dataset doesnt cover the case where a subset of submitted tables matches a dataset
    # if a subset of submitted tables matches a dataset, but there is one or more submitted tabs that do not match
    #  then core checks etc. will run, but the app will crash, since there is a tab that does not match any table
    # We now will require a dataset to match all the tablenames in the match report. Note that the tablename will be empty if no table was matched for a certain tab
    # In this case, 
    match_dataset = [k for k,v in datasets.items() if set(v.get('tables')) == set([x.get('tablename') for x in match_report])]

    

    assert len(match_dataset) < 2, "matched 2 or more different datasets, which should never happen"

    if len(match_dataset) == 0:
        print("No dataset matched")

        # TODO We can make a routine to find the closest match - Robert
        # that probably isnt going to happen unless a user brings up that point that it would help
        # https://preview.redd.it/nvrpt44due141.jpg?auto=webp&s=ff39d49780b44fab66bb0bbd6fb0e9af0244974d

        match_dataset = ""
    else:
        # first have to confirm it found something before extracting that key of the match dataset variable
        # which is the name of the dataset
        match_dataset = match_dataset[0]



    # drop from all_dfs whatever was not matched
    if match_dataset == 'toxicity':
        for sheetname in list(set(all_dfs.keys()) - set(matched_tables)):
            del all_dfs[sheetname]

        # also another thing for the sake of toxicity data
        # Dont include the summary tab in the match report, it gets re calculated anyways
        match_report = [r for r in match_report if r.get('closest_tbl') != current_app.config.get("TOXSUMMARY_TABLENAME")]


    print("match report")
    print(match_report)
    
    # As stated earlier, this is to be used later by the browser to display error messages as associated with the excel tab name
    # Rather than the database tablename which it matched
    # NOTE We may also be able to use this to rename the keys of "all_dfs" so that we dont have to alter the excel file's tab names
    session['table_to_tab_map'] = table_to_tab_map

    return match_dataset, match_report, all_dfs


    