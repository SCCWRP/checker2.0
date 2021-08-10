import multiprocessing as mp
import pandas as pd
import time
from itertools import chain
from .dupes import checkDuplicatesInSession, checkDuplicatesInProduction
from .lookups import checkLookUpLists
from .metadata import checkNotNull, checkPrecision, checkScale, checkLength, checkDataTypes, checkIntegers
from .functions import fetch_meta, multitask


# goal here is to take in all_dfs as an argument and assemble the CoreChecker processes
def core(all_dfs, eng, all_meta, debug = False):

    errs = []
    warnings = []
    for tbl, df in all_dfs.items():
        print(tbl)
        errs.extend(
            [
                checkDuplicatesInSession(df, tbl, eng, all_meta[tbl]),
                checkDuplicatesInProduction(df, tbl, eng, all_meta[tbl]),
                checkLookUpLists(df, tbl, eng, all_meta[tbl]),
                checkNotNull(df, tbl, eng, all_meta[tbl]),
                checkIntegers(df, tbl, eng, all_meta[tbl]),
                checkPrecision(df, tbl, eng, all_meta[tbl]),
                checkLength(df, tbl, eng, all_meta[tbl]),
                checkDataTypes(df, tbl, eng, all_meta[tbl])
            ]
            
            if debug 
            else
            
            multitask(
                [
                    checkDuplicatesInSession,
                    checkDuplicatesInProduction,
                    checkLookUpLists,
                    checkNotNull,
                    checkIntegers,
                    checkPrecision,
                    checkLength,
                    checkDataTypes
                ],
                df,
                tbl,
                eng,
                all_meta[tbl]
            )
        )

        warnings.extend(
            [checkScale(df, tbl, eng, all_meta[tbl])]
            if debug 
            else
            multitask([checkScale], df, tbl, eng, all_meta[tbl])
        )

    # flatten the lists
    print(errs)
    print(warnings)
    return {
        "core_errors": [e for sublist in errs for e in sublist if ( e != dict() and e != set() )],
        "core_warnings": [w for sublist in warnings for w in sublist if ( w != dict() and w != set() )]
    }
    
