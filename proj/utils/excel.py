import os, shutil
from openpyxl import load_workbook
from openpyxl.comments import Comment
from openpyxl.styles import PatternFill
from flask import session


def mark_workbook(all_dfs, excel_path, errs, warnings):
    assert session.get('submission_dir') is not None, "function - mark_workbook - session submission dir is not defined."
    orig_filename = excel_path.rsplit('/', 1)[-1]
    filename = orig_filename.split('.')[0]
    ext = orig_filename.split('.')[-1]
    marked_path = os.path.join(session.get('submission_dir'), f"{filename}-marked.{ext}")
    
    # copy the excel file and have "marked" in the name, and we will mark this excel file
    shutil.copy(excel_path, marked_path)

    errs_cells = dict()
    for table in set([e.get('table') for e in errs]):
        errs_cells[table] = []
    [    
        errs_cells[table].append(
            {
                'row_index': r.get('row_number'),
                'column_index': all_dfs[table].columns.get_loc(str(col).strip()),
                'message': r.get('message')
            }
        )
        for e in errs
        for col in e.get('columns').split(',')
        for r in e.get('rows')
    ]

    warnings_cells = dict()
    for table in set([w.get('table') for w in warnings]):
        warnings_cells[table] = []
    [    
        warnings_cells[table].append(
            {
                'row_index': r.get('row_number'),
                'column_index': all_dfs[table].columns.get_loc(str(col).strip()),
                'message': r.get('message')
            }
        )
        for w in warnings
        for col in w.get('columns').split(',')
        for r in w.get('rows')
    ]

    
    # for errors
    redFill = PatternFill(
        start_color='FFFF0000',
        end_color='FFFF0000',
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
        for coord in errs_cells.get(sheet) if errs_cells.get(sheet) is not None else []:
            wb[sheet][f"{chr(65 + int(coord.get('column_index')))}{coord.get('row_index')}"].fill = redFill 
            wb[sheet][f"{chr(65 + int(coord.get('column_index')))}{coord.get('row_index')}"].comment = Comment(coord.get('message'), "Checker")
        for coord in warnings_cells.get(sheet) if warnings_cells.get(sheet) is not None else []:
            wb[sheet][f"{chr(65 + int(coord.get('column_index')))}{coord.get('row_index')}"].fill = yellowFill 
            wb[sheet][f"{chr(65 + int(coord.get('column_index')))}{coord.get('row_index')}"].comment = Comment(coord.get('message'), "Checker")

    wb.save(marked_path)