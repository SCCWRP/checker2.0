import os, shutil
from flask import session
def highlight_changes(worksheet, color, cells):
    """
        worksheet: the excel worksheet being formatted,
        color: the format that will be added,
        cells: list of tuples, indicating the coordinates of the cells to be highlighted

        This function aims to utilie the speed of the list comprehension for loop, without returning anything
    """
    [
        worksheet.conditional_format(
            # coord is a tuple of the coordinates of cells that were changed, and need to be highlighted
            coord.get('row_index'), coord.get('column_index'), coord.get('row_index'), coord.get('column_index'),
            {
                'type': 'no_errors',
                'format': color
            }
        )
        for coord in cells
    ]
    return None

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

    print("warnings_cells")
    print(warnings_cells)
    print("errs_cells")
    print(errs_cells)
