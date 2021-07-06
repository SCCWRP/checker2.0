import multiprocessing as mp
import pandas as pd
import time
from itertools import chain
from .dupes import checkDuplicatesInSession
from .lookups import checkLookUpLists
from .metadata import checkNotNull, checkPrecision, checkScale, checkLength, checkDataTypes
from .functions import fetch_meta, multitask

class CoreChecker(mp.Process):
    def __init__(self, dataframe, tablename, eng, meta, col, func, q = None):
        mp.Process.__init__(self)
        print("init")
        self.dataframe = dataframe
        self.tablename = tablename
        self.eng = eng
        self.meta = meta
        self.col = col
        self.func = func
        self.errors_list = []
        self.q = q
    
    def run(self):
        print("running")
        self.errors_list.append(
            self.func(
                dataframe = self.dataframe,
                tablename = self.tablename,
                eng = self.eng,
                meta = self.meta,
                col = self.col
            )
        )
        print("done")

# goal here is to take in all_dfs as an argument and assemble the CoreChecker processes
def core(all_dfs, eng, all_meta):

    debug = False
    # to run with no multiprocessing
    errs = []
    if debug:
        for tbl, df in all_dfs.items():
            print(tbl)
            errs.extend(
                [
                    checkDuplicatesInSession(df, tbl, eng, all_meta[tbl]),
                    checkLookUpLists(df, tbl, eng, all_meta[tbl]),
                    checkNotNull(df, tbl, eng, all_meta[tbl]),
                    checkPrecision(df, tbl, eng, all_meta[tbl]),
                    checkScale(df, tbl, eng, all_meta[tbl]),
                    checkLength(df, tbl, eng, all_meta[tbl]),
                    checkDataTypes(df, tbl, eng, all_meta[tbl])
                ]
            )
            print("done")
        print("core checks done")
        return errs
    else:
        for tbl, df in all_dfs.items():
            errs.extend(
                multitask(
                    [
                        checkDuplicatesInSession,
                        checkLookUpLists,
                        checkNotNull,
                        checkPrecision,
                        checkScale,
                        checkLength,
                        checkDataTypes
                    ],
                    df,
                    tbl,
                    eng,
                    all_meta[tbl]
                )
            )
        return errs




    # # Lookup Lists
    # processes = \
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = None,
    #             func = checkLookUpLists
    #         )
    #         for tbl, df in all_dfs.items()
    #     ]
    

    # # Dupes
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = None,
    #             func = checkDuplicatesInSession
    #         )
    #         for tbl, df in all_dfs.items()
    #     ]
    # )
    
    # # Data Types
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = col,
    #             func = checkDataTypes
    #         )
    #         for tbl, df in all_dfs.items()
    #         for col in df.columns
    #     ]
    # )

    # # Numeric Precision
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = col,
    #             func = checkPrecision
    #         )
    #         for tbl, df in all_dfs.items()
    #         for col in df.columns 
    #         if col in all_meta[tbl][all_meta[tbl].udt_name == 'numeric'].column_name.values
    #     ]
    # )

    # # Numeric Scale
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = col,
    #             func = checkScale
    #         )
    #         for tbl, df in all_dfs.items()
    #         for col in df.columns if col in all_meta[tbl][all_meta[tbl].udt_name == 'numeric'].column_name.values
    #     ]
    # )

    # # Character Length
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = col,
    #             func = checkLength
    #         )
    #         for tbl, df in all_dfs.items()
    #         for col in df.columns 
    #         if col in all_meta[tbl][~pd.isnull(all_meta[tbl].character_maximum_length)].column_name.values
    #     ]
    # )
    
    # # Not NULL
    # processes.extend(
    #     [
    #         CoreChecker(
    #             dataframe = df, 
    #             tablename = tbl, 
    #             eng = eng, 
    #             meta = all_meta[tbl],
    #             col = col,
    #             func = checkNotNull
    #         )
    #         for tbl, df in all_dfs.items()
    #         for col in df.columns 
    #         if col in all_meta[tbl][all_meta[tbl].is_nullable == 'NO'].column_name.values
    #     ]
    # )

    # print("processes")
    # print(processes)

    # for p in processes:
    #     print("starting process")
    #     p.start()

    # time.sleep(2)
    # for p in processes:
    #     print("joining process")
    #     p.join()
    #     print(p.errors_list)
    # return list(chain(*[p.errors_list for p in processes]))