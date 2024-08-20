from inspect import currentframe
import pandas as pd

# Typically filter condition should be the analytename restricted to certain values
def chk_required_sampletypes(df, sampletypes, analyteclass, additional_grouping_cols = [], row_index_col = 'tmp_row'):

    # Goal: Construct a list of args (dictionary format) to be passed iteratively into the checkData function in chemistry_custom{
    # {
    #     "badrows": [list of indices],
    #     "badcolumn": "SomeBadColumn",
    #     "error_type": "Missing Required Data",
    #     "error_message": "Some error message"
    # }

    assert row_index_col in df.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"

    assert type(additional_grouping_cols) == list, "additional_grouping_cols arg is not a list"
    grouping_columns = ['analysisbatchid', 'analytename', *additional_grouping_cols]

    # Assertions - for faster debugging if the function does not get used correctly for whatever reason
    assert set(grouping_columns).issubset(set(df.columns)), f"grouping_columns list ({','.join(grouping_columns)}) not all found in columns of the dataframe"
    assert 'sampletype' in df.columns, "dataframe does not have 'sampletype' column"
    assert type(sampletypes) == list, "grouping_columns arg is not a list"
    assert isinstance(df, pd.DataFrame), "df arg is not a pandas DataFrame"
    assert isinstance(analyteclass, str)

    tmpdf = df[df.analyteclass == analyteclass]

    # If it comes up empty, that analyteclass is not part of this submission, so there's nothing to check.
    if tmpdf.empty:
        return []

    print(tmpdf)
    tmpdf = tmpdf.groupby(grouping_columns).apply(
        lambda subdf:
        set(sampletypes) - set(subdf.sampletype.unique())
    ) 

    assert (len(tmpdf) > 0), f"for some reason, after grouping by {','.join(grouping_columns)}, the dataframe came up empty (in { str(currentframe().f_code.co_name)} function)"
    
    tmpdf = tmpdf.reset_index(name = 'missing_sampletypes')

    tmpdf = tmpdf[tmpdf.missing_sampletypes != set()]

    if tmpdf.empty:
        # No errors found
        args = []
    else:
        checkdf = df.merge(tmpdf, on = grouping_columns, how = 'inner')
        checkdf = checkdf[[*grouping_columns, 'missing_sampletypes', row_index_col]]
        checkdf.missing_sampletypes = checkdf.missing_sampletypes.apply(lambda x: ','.join(x))
        checkdf = checkdf.groupby([*grouping_columns, 'missing_sampletypes']).agg({'tmp_row':list}).reset_index()

        args = checkdf.apply(
            lambda row: 
            {
                "badrows": row.tmp_row,
                "badcolumn": "SampleType",
                "error_type": "Missing Required Data",
                "error_message": f"For the AnalysisBatch {row.analysisbatchid} and Analyte {row.analytename}, you are missing the required sampletypes: {row.missing_sampletypes}"
            },
            axis = 1
        ).tolist()

    return args


# Typically filter condition should be the analytename restricted to certain values
def check_required_crm(df, analyteclasses, grouping_columns = ['analysisbatchid', 'analyteclass'], row_index_col = 'tmp_row'):

    # Goal: Construct a list of args (dictionary format) to be passed iteratively into the checkData function in chemistry_custom{
    # {
    #     "badrows": [list of indices],
    #     "badcolumn": "SomeBadColumn",
    #     "error_type": "Missing Required Data",
    #     "error_message": "Some error message"
    # }
    # Need to check analysis batches and analyteclasses that are missing CRM's. 
    # Only Analyteclasses which require CRM's should be in that analyteclasses list argument
    
    if df.empty:
        return []
    
    tmpdf = df[df.analyteclass.isin(analyteclasses)]
    if tmpdf.empty:
        return []
    
    assert isinstance(analyteclasses, list), "analyteclasses arg is not a list"

    assert row_index_col in tmpdf.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"


    tmpdf = tmpdf.groupby(grouping_columns).apply(
        lambda subdf: 
        not any(subdf.sampletype.apply(lambda x: 'reference' in str(x).lower()))
    )
    assert not tmpdf.empty, \
        f"for some reason, after grouping by {','.join(grouping_columns)}, the dataframe came up empty (in {str(currentframe().f_code.co_name)} function)"
    
    tmpdf = tmpdf.reset_index(name = 'missing_crm')

    tmpdf = tmpdf[tmpdf.missing_crm]

    if tmpdf.empty:
        # No errors found
        args = []
    else:
        # checkdf - just a temp variable only intended to be used in this little code block
        checkdf = df.merge(tmpdf, on = grouping_columns, how = 'inner')
        checkdf = checkdf[[*grouping_columns, 'missing_crm', row_index_col]]
        checkdf.missing_crm = checkdf.missing_crm.apply(lambda x: str(x))
        checkdf = checkdf.groupby([*grouping_columns, 'missing_crm']).agg({'tmp_row':list}).reset_index()

        args = checkdf.apply(
            lambda row: 
            {
                "badrows": row.tmp_row,
                "badcolumn": "SampleType",
                "error_type": "Missing Required Data",
                "error_message": f"For the AnalysisBatch {row.analysisbatchid} and AnalyteClass {row.analyteclass}, you are missing a Certified Reference Material"
            },
            axis = 1
        ).tolist()
        
        del checkdf

    return args

def pyrethroid_analyte_logic_check(df, analytes, row_index_col = 'tmp_row'):
    assert 'analysisbatchid' in df.columns, 'analysisbatchid not found in columns of the dataframe'
    assert 'analytename' in df.columns, 'analytename not found in columns of the dataframe'
    assert 'analyteclass' in df.columns, 'analyteclass not found in columns of the dataframe'
    assert row_index_col in df.columns, \
        f"{row_index_col} not found in the columns of the dataframe. \
        It is highly recommended you create a tmp_row column in the dataframe by resetting the index, \
        otherwise you run the risk of incorrectly reporting the rows that contain errors, \
        due to the nature of these checks which require merging and groupby operations with multiple dataframes"

    tmp = df[df.analyteclass == 'Pyrethroid'].groupby('analysisbatchid').apply(
        lambda df:
        set(analytes).issubset(set(df.analytename.unique()))
    )
    if not tmp.empty:
        tmp = tmp.reset_index(name = 'failed_check')
        tmp = df.merge(tmp, on = 'analysisbatchid', how = 'inner')
        badrows = tmp[tmp.analytename.isin(analytes)][row_index_col].tolist()
    else:
        badrows = []

    return {
        "badrows": badrows,
        "badcolumn": "AnalysisBatchID, AnalyteName",
        "error_type": "Logic Error",
        "error_message": f"This batch contains both/all of {','.join(analytes)} which is not possible"
    }


# def check_req_analytes(df, mask, groupingcols, required_analytes, analyteclass):
#     # Each stationid should have all required analytes if they have one from 

#     assert 'tmp_row' in df.columns, \
#         "in check_req_analytes - tmp_row column not defined in the dataframe - incorrect rows may be reported - aborting"
#     assert set(groupingcols).issubset(set(df.columns)), \
#         f"in check_req_analytes - grouping columns {', '.join(groupingcols)} not a subset of the dataframe's columns"

#     assert "analytename" in df.columns, \
#         f"dataframe has no column named analytename (in check required analytes function"

#     # initialize return value here, if no errors are found, an empty list will get returned
#     arglist = []

#     tmp = df[mask].groupby(groupingcols).apply(lambda df: set(required_analytes) - set(df.analytename.unique()) )
#     if not tmp.empty:
#         tmp = tmp.reset_index(name = 'missing_analytes')
#         tmp = df.merge(tmp, on = groupingcols, how = 'inner')
#         tmp = tmp[tmp.missing_analytes != set()]
#         if not tmp.empty:
#             tmp.missing_analytes = tmp.missing_analytes.apply(lambda anlts: ','.join(anlts))
#             tmp = tmp \
#                 .groupby([*groupingcols,'missing_analytes']) \
#                 .apply(lambda df: df.tmp_row.tolist()) \
#                 .reset_index(name = 'badrows')

#             arglist = tmp.apply(
#                 lambda row:
#                 {
#                     "badrows": row.badrows,
#                     "badcolumn": "AnalyteName",
#                     "error_type": "Missing Required Data",
#                     "error_message": f"""For the grouping of {', '.join(['{}: {}'.format(x, row[x]) for x in groupingcols])}, you are missing the following required Analytes (For the {analyteclass} Analyteclass): {row.missing_analytes}"""
#                 },
#                 axis = 1
#             ).tolist()

#     return arglist

# Lab blanks - result has to be less than MDL, or less than 5% of the measured concentration in the sample
def MB_ResultLessThanMDL(dataframe):
    """
    dataframe should have the tmp_row column, and already be filtered according to the "table" (table 5-3, 5-4 etc)
    This function is supposed to return a list of errors
    """
    print("IN MB_ResultLessThanMDL")

    print("filter to methodblank samples and select only 5 needed columns")
    methodblanks = dataframe[dataframe.sampletype == 'Lab blank'][['analysisbatchid','analytename','mdl', 'result','tmp_row']]
    print("filter to Results to merge with methodblanks")
    res = dataframe[dataframe.sampletype == 'Result']

    print('merge')
    checkdf = res.merge(methodblanks, on = ['analysisbatchid','analytename'], how = 'inner', suffixes = ('','_mb'))

    print('filter to rows with too high methodblank values')
    checkdf = checkdf[
        ~checkdf.apply(
            lambda row: 
            (row.result_mb < row.mdl_mb) | (row.result_mb < (0.05 * row.result)) 
            , axis = 1
        )
    ]

    if checkdf.empty:
        return []
    print('get bad rows for updating the args')
    checkdf = checkdf.groupby(['analysisbatchid','analytename','sampleid']).apply(lambda df: list(set(df.tmp_row_mb.tolist())) ).reset_index(name = 'badrows')

    args = checkdf.apply(
            lambda row:
            {
                "badrows": row.badrows,
                "badcolumn": "Result",
                "error_type": "Value Error",
                "error_message": f"For the Analyte {row.analytename} in the AnalysisBatch {row.analysisbatchid}, the Lab blank result value is either above the MDL, or above 5% of the measured concentration in the sample {row.sampleid}"
            },
            axis = 1
        ).tolist()
    print("done")
    print("END MB_ResultLessThanMDL")

    return args

def check_dups(df, analyteclass, sampletype):
    # Each analysis batch needs some kind of duplicate
    # Inorganics - Duplicate Matrix Spike or Sample Duplicate (Duplicate Results)
    # Organics - Duplicate Matrix Spike
    # Total Organic Carbon - Sample Duplicate (Duplicate Results)
    # Total Nitrogen - Sample Duplicate (Duplicate Results)
    print(str(currentframe().f_code.co_name))
    args = []

    tmpdf = df[(df.sampletype == sampletype) & (df.analyteclass == analyteclass)]

    if tmpdf.empty:
        print(f"END {str(currentframe().f_code.co_name)}")
        return []

    tmpdf = tmpdf.groupby(['analysisbatchid','sampleid','sampletype']).apply(
            lambda subdf:
            all(subdf.groupby('analytename').apply(lambda d: set(d.labreplicate.tolist()) == {1,2}))
        )\
        .reset_index(name = 'has_dup') \
        .groupby('analysisbatchid') \
        .apply(
            # checking if within a batch, there was at least one sampleid that had all their dupes
            lambda subdf:
            any(subdf.has_dup.values)
        ) \
        .reset_index(
            # previous "passed" column would have disappeared
            name = 'passed'
        ) \
        .merge(df, on = ['analysisbatchid'])


    baddf = tmpdf[~tmpdf.passed]

    if baddf.empty:
        print(f"END {str(currentframe().f_code.co_name)}")
        return []
    else:
        baddf['errmsg'] = baddf.apply(
            lambda row: 
            f"""The AnalysisBatch {row.analysisbatchid} is missing a duplicate {sampletype} (for the compound class {analyteclass})"""
            , axis = 1
        )

        args = baddf.groupby(['analysisbatchid','errmsg']) \
            .apply(lambda df: df.tmp_row.tolist()) \
            .reset_index(name = 'badrows') \
            .apply(
                lambda row: 
                {
                    "badrows": row.badrows,
                    "badcolumn": "AnalysisBatchID",
                    "error_type": "Value Error",
                    "error_message": row.errmsg
                },
                axis = 1
            ).tolist()
    
    print(f"END {str(currentframe().f_code.co_name)}")
    return args
    

