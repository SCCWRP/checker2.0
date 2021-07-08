
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
            coord[0], coord[1], coord[0], coord[1],
            {
                'type': 'no_errors',
                'format': color
            }
        )
        for coord in cells
    ]
    return None