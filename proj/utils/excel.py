import os, shutil
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import PatternFill
from flask import session
from math import floor


def mark_workbook(all_dfs, excel_path, errs, warnings):
    assert session.get('submission_dir') is not None, "function - mark_workbook - session submission dir is not defined."
    orig_filename = excel_path.rsplit('/', 1)[-1]
    filename = orig_filename.split('.')[0]
    ext = orig_filename.split('.')[-1]
    marked_path = os.path.join(session.get('submission_dir'), f"{filename}-marked.{ext}")
    
    # copy the excel file and have "marked" in the name, and we will mark this excel file
    shutil.copy(excel_path, marked_path)

    # No empty errors allowed otherwise it crashes
    errs = [e for e in errs if len(e) > 0]

    errs_cells = dict()
    for table in set([e.get('table') for e in errs]):
        errs_cells[table] = []
        [    
            errs_cells[table].append(
                {
                    'row_index': r,
                    'column_index': all_dfs[table].columns.get_loc(str(col).strip()),
                    'message': e.get('error_message')
                }
            )
            for e in errs
            for col in e.get('columns').split(',')
            for r in e.get('rows')
            if e.get('table') == table
        ]

    warnings_cells = dict()
    for table in set([w.get('table') for w in warnings]):
        warnings_cells[table] = []
        [    
            warnings_cells[table].append(
                {
                    'row_index': r,
                    'column_index': all_dfs[table].columns.get_loc(str(col).strip()),
                    'message': f"{w.get('error_message')} (Warning)"
                }
            )
            for w in warnings
            for col in w.get('columns').split(',')
            for r in w.get('rows')
            if w.get('table') == table
        ]

    
    # for errors
    redFill = PatternFill(
        start_color='FF8585',
        end_color='FF8585',
        fill_type='solid'
    )

    # for warnings
    yellowFill = PatternFill(
        start_color='00FFFF00',
        end_color='00FFFF00',
        fill_type='solid'
    )


    wb = load_workbook(marked_path)

    for sheet in wb.sheetnames:
        # Mark warnings first - this way if there are an error and warning in the same cell, the error will be shown
        for coord in warnings_cells.get(sheet) if warnings_cells.get(sheet) is not None else []:
            colindex = coord.get('column_index')
            wb[sheet][f"{chr(65 +  (floor(colindex/26) - 1)  ) if colindex >= 26 else ''}{chr(65 + (colindex % 26))}{coord.get('row_index')}"].fill = yellowFill 
            wb[sheet][f"{chr(65 +  (floor(colindex/26) - 1)  ) if colindex >= 26 else ''}{chr(65 + (colindex % 26))}{coord.get('row_index')}"].comment = Comment(coord.get('message'), "Checker")
        for coord in errs_cells.get(sheet) if errs_cells.get(sheet) is not None else []:
            
            # the workbook sheet or whatever its called accesses the cells of the excel file not with the numeric indexing like pandas but rather that letter indexing thing
            # like "Cell A1" and stuff like that

            # So the gigantic disgusting f string f"{chr(65 +  (math.floor(colindex/26) - 1)  ) if colindex >= 26 else ''}{chr(65 + (colindex % 26))}{coord.get('row_index')}"
            # is to convert from pandas indexing to the letter indexing style thing

            colindex = coord.get('column_index')
            wb[sheet][f"{chr(65 +  (floor(colindex/26) - 1)  ) if colindex >= 26 else ''}{chr(65 + (colindex % 26))}{coord.get('row_index')}"].fill = redFill 
            wb[sheet][f"{chr(65 +  (floor(colindex/26) - 1)  ) if colindex >= 26 else ''}{chr(65 + (colindex % 26))}{coord.get('row_index')}"].comment = Comment(coord.get('message'), "Checker")
        

    wb.save(marked_path)

    return marked_path