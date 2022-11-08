import pandas as pd
import re
from math import log10
from .functions import checkData, convert_dtype, fetch_meta, check_precision, check_length, check_scale
from flask import current_app


def checkDataTypes(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkDataTypes")
    ret = []
    for col in dataframe.columns: 
        if col not in current_app.system_fields:
            # using the meta dataframe we can get the python datatype
            dtype = meta.iloc[
                meta[
                    meta.column_name == col
                ].index, 
                meta.columns.get_loc("dtype")
            ] \
            .values[0]

            human_dtype = meta.iloc[
                    meta[
                        meta.column_name == col
                    ].index, 
                    meta.columns.get_loc("data_type")
                ] \
                .values[0]

            ret.append(
                checkData(
                    dataframe = dataframe,
                    tablename = tablename,
                    badrows = dataframe[
                            dataframe[col].apply(
                                lambda x:
                                # function returns True if it successfully converted
                                # We negate to get the cases where it was not successfully converted
                                not convert_dtype(dtype, x)
                            )
                        ].index.tolist(),
                    badcolumn = col,
                    error_type = "Invalid Datatype",
                    is_core_error = True,
                    error_message = f'''The value here is not valid for the datatype "{human_dtype}"'''
                )
            )
            #print("ret:")
            #print(ret)
        

    print("-----before if output-----")
    print("dataframe.columns")
    print(dataframe.columns)
    if output:
        print("---enter if output---")
        output.put(ret)
    print("END checkDataTypes")
    return ret
    

def checkPrecision(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkPrecision")
    ret = []
    for col in dataframe.columns:
        if (
            (col in meta[meta.udt_name == 'numeric'].column_name.values)
            and (col not in current_app.system_fields)
        ):

            prec = int(
                meta.iloc[
                    meta[
                        meta.column_name == col
                    ].index, 
                    meta.columns.get_loc("numeric_precision")
                ] \
                .values[0]
            )

            ret.append(
                checkData(
                    dataframe = dataframe,
                    tablename = tablename,
                    badrows = dataframe[
                        dataframe[col].apply(
                            lambda x:
                            not check_precision(x,prec)
                        )
                    ].index.tolist(),
                    badcolumn = col,
                    error_type = "Value too long",
                    is_core_error = True,
                    error_message = f"the value here is too long for the column {col} which allows {prec} significant digits"
                )
            )

    if output:
        output.put(ret)
    print("END checkPrecision")
    return ret

def checkScale(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkScale")
    ret = []
    for col in dataframe.columns:
        if (
            (col in meta[meta.udt_name == 'numeric'].column_name.values)
            and (col not in current_app.system_fields)
        ):
            scale = int(
                meta.iloc[
                    meta[
                        meta.column_name == col
                    ].index, 
                    meta.columns.get_loc("numeric_scale")
                ] \
                .values[0]
            )

            ret.append(
                checkData(
                    dataframe = dataframe,
                    tablename = tablename,
                    badrows = dataframe[
                        dataframe[col].apply(
                            lambda x:
                            not check_scale(x,scale)
                        )
                    ].index.tolist(),
                    badcolumn = col,
                    error_type = "Value too long",
                    is_core_error = True,
                    error_message = f"The value here will be rounded to {scale} decimal places when it is loaded to the database"
                )
            )
        

    if output:
        output.put(ret) 
    print("END checkScale")
    # Getting warning to round salinity_ppt for ctd_data (sensor data) when value has fewer than 15 decimal places...
    print("ret for checkScale")
    print(ret)
    return ret


def checkLength(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkLength")

    # ret for return, or the item that will be returned
    ret = []
    for col in dataframe.columns:
        if (
            (col in meta[~pd.isnull(meta.character_maximum_length)].column_name.values)
            and (col not in current_app.system_fields)
        ):
            maxlen = int(
                meta.iloc[
                    meta[
                        meta.column_name == col
                    ].index, 
                    meta.columns.get_loc("character_maximum_length")
                ] \
                .values[0]
            )

            ret.append(
                checkData(
                    dataframe = dataframe,
                    tablename = tablename,
                    badrows = dataframe[(~pd.isnull(dataframe[col])) & (dataframe[col].astype(str).str.len() > maxlen)].index.tolist(),
                    badcolumn = col,
                    error_type = "Value too long",
                    is_core_error = True,
                    error_message = f"The value here has too many characters, while the character limit is {maxlen}"
                )
        
            )

    if output:
        output.put(ret) 
    print("END checkLength")
    return ret



def checkNotNull(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkNotNULL")

    if 'sampleid' in dataframe.columns:
        print(dataframe.sampleid)
        import time
        time.sleep(3)
    ret = \
    [
        checkData(
            dataframe = dataframe,
            tablename = tablename,
            badrows = dataframe[
                    dataframe[col].apply(
                        lambda x:
                        True if ((pd.isnull(x)) or (x == '') or (str(x).lower() == 'nan')) else False
                    )
                ].index.tolist(),
            badcolumn = col,
            error_type = "Missing Required Data",
            is_core_error = True,
            error_message = f"There is an empty value here, but the column {col} requires a value in all rows"
        )
        for col in dataframe.columns 
        if (
            (col in meta[meta.is_nullable == 'NO'].column_name.values)
            and (col not in current_app.system_fields)
        )
    ]

    if output:
        output.put(ret) 

    print("END checkNotNULL")
    return ret



def checkIntegers(dataframe, tablename, eng, meta, *args, output = None, **kwargs):
    print("BEGIN checkIntegers")
    ret = []
    for col in dataframe.columns:
        if (
                (col in meta[meta.udt_name.isin(['int2','int4','int8'])].column_name.values)
                and (col not in current_app.system_fields)
        ):
            udt_name = meta.iloc[meta[meta.column_name == col].index, meta.columns.get_loc("udt_name")].values[0]

            # Need to check if all the values are valid int literals first
            if not all(
                dataframe[col] \
                    .apply(
                        lambda x:
                        # function returns True if it successfully converted
                        convert_dtype(int, x)
                    ) \
                    .values
            ):
                continue


            ret.append(
                checkData(
                    dataframe = dataframe,
                    tablename = tablename,
                    badrows = dataframe[
                            dataframe[col].apply(
                                lambda x:
                                False if pd.isnull(x)
                                else not ( (int(x) >= -32768) & (int(x) <= 32767) )
                                if udt_name == 'int2'
                                else not ( (int(x) >= -2147483648) & (int(x) <= 2147483647) )
                                if udt_name == 'int4'
                                else not ( (int(x) >= -9223372036854775808) & (int(x) <= 9223372036854775807) )
                                if udt_name == 'int8'
                                
                                # if something else slips through the cracks, this will not allow it through by default
                                else True 
                            )
                        ].index.tolist(),
                    badcolumn = col,
                    error_type = "Value out of range",
                    is_core_error = True,
                    error_message = "The column {} allows integer values from {}" \
                        .format(
                            col,
                            "-32768 to 32767"
                            if udt_name == 'int2'
                            else  "-2147483648 to 2147483647"
                            if udt_name == 'int4'
                            else  "-9223372036854775808 to 9223372036854775807"
                            if udt_name == 'int8'

                            # It should never be anything other than the above cases, since below we are filtering for int2, 4, and 8 columns.
                            else "(unexpected error occurred. If you see this, contact it@sccwrp.org)"
                        )
                )
            )
         
       

    if output:
        output.put(ret)
    print("END checkIntegers")
    return ret