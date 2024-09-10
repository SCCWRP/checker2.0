# Templater routine dynamically calls lookup lists from database to populate into the data templates.
# The updated templates will be outputted to the user with all tables and the lookup lists. 
#below is from microplastics
#from flask import send_from_directory, render_template, request, redirect, Response, jsonify, send_file, json, current_app
#below is from empa main.py
from flask import request, current_app, Blueprint, g, send_file, make_response, flash, render_template
from sqlalchemy import Table, MetaData
import pandas as pd
from pandas import DataFrame
import re
import os
import openpyxl
from openpyxl.utils import get_column_letter, quote_sheetname
from openpyxl.styles import PatternFill, Font
from openpyxl.styles.alignment import Alignment
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.formatting.rule import FormulaRule
from openpyxl.comments import Comment
from io import BytesIO

from .utils.db import primary_key, foreign_key_detail, get_column_comments

# dynamic lookup lists to template
# skip the formatting

templater = Blueprint('templater', __name__)


@templater.route('/templates', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
@templater.route('/templater', methods = ['GET', 'POST']) # this will be added to the index.html file to dynamically call the lookup lists to each template
# consider using the app.datasets dictionary to generalize the code better
def template():
    system_fields = current_app.system_fields
    datatype = request.args.get("datatype")

    if datatype not in current_app.datasets.keys():
        if datatype is not None:
            flash(f"Datatype {datatype} not found")
        return render_template(
            "templates.jinja2",
            datasets = current_app.datasets,
            project_name = current_app.project_name,
            background_image = current_app.config.get("BACKGROUND_IMAGE")
        )
    
    # All tables in the dataset
    tbls = current_app.datasets.get(datatype)['tables']
    
    file_prefix = datatype.upper()
    database_name = str(g.eng).replace(")","").split("/")[-1]
    print(current_app.datasets.keys())

    static_template = current_app.datasets.get(datatype).get('template_filename')
    if static_template is not None:
        print("Static template requested")
        return send_file(
            os.path.join(os.getcwd(), "export", "data_templates", static_template), as_attachment=True, download_name=f'{static_template}'
        )
    
    eng = g.eng

    tabs_dict = {}

    for tbl in tbls:
        print('tbl')
        print(tbl)
        # foreign key detail returns a dictionary in records fashion rather than just columns and which tables they reference
        # foreign key detail gives the name of the referenced column as well
        
        pkey_fields = primary_key(tbl, eng)
        fkey_detail = foreign_key_detail(tbl, eng)
        print('fkey_detail')
        print(fkey_detail)
        tabs_dict[tbl] = {
            'pkey_fields' : pkey_fields,
            'foreign_key_detail' : fkey_detail,
            'foreign_key_tables' : list({info['referenced_table'] for info in fkey_detail.get(tbl, {}).values()}),
            'constrained_columns' : list(fkey_detail.get(tbl, {}).keys())
        }
        
        print("tabs dict")
        print( tabs_dict[tbl] )
    
    lookup_tables = [t for v in tabs_dict.values() for t in v.get('foreign_key_tables')]
    
    # Build the dictionary which will be the main object that creates the data template
    xls = {
        **{
            'Instructions': pd.DataFrame(
                
                    {
                        'How to use:': [
                            "Information about this spreadsheet:",
                            "SCCWRP spreadsheets follow a standard format, each consisting of several sheets: data templates, lookup lists, and a glossary.",
                            "Metadata for each column can be found by selecting the header cell for that column, or in the glossary sheet. Please do not add or rename columns. Use note columns to provide additional information or context.",
                            "Questions or comments? Please contact Paul Smith at pauls@sccwrp.org"
                        ]
                    }
                
            )
        },
        **{
            table: pd.DataFrame(
                columns=
                [
                    *[
                        x for x in pd.read_sql(
                            """
                                SELECT {} FROM {} LIMIT 1
                            """.format(
                                ','.join(pd.read_sql(f"SELECT column_name FROM column_order WHERE table_name = '{table}' ORDER BY custom_column_position;", eng).column_name.tolist()),
                                table
                            ),
                            eng
                        ).columns.to_list()
                        if x not in system_fields
                    ]
                ]
            ) for table in tbls # remember, tbls was defined towards the beginning, and represents all tables in the dataset
        },
        **{
            'glossary': pd.read_sql(f"""SELECT * FROM vw_template_glossary WHERE tablename IN ('{"','".join(tbls)}') ;""", eng)
        },
        **{
            lu_name: pd.read_sql(f"SELECT * from {lu_name}", eng).drop(columns=['objectid'], errors = 'ignore')
            for lu_name in list(set(lookup_tables))
        }
    }
    
    excel_blob = BytesIO()
        
    with pd.ExcelWriter(excel_blob, engine='openpyxl') as writer:
        # Gray highlight format
        
        
        
        FKEY_HIGHLIGHT = PatternFill(start_color="D7D6D6", end_color="D7D6D6", fill_type="solid")
        PKEY_BOLD_FONT = Font(bold=True)
        
        try:
            rotation = int(current_app.config.get("TEMPLATE_COLUMN_HEADER_ROTATION", 90))
            COL_HEADER_ROTATION = Alignment(text_rotation = rotation , horizontal='center', vertical='center')
        except Exception as e:
            print("Warning: Couldnt set custom column header rotation - likely an error in the app configuration - defaulting to 90")
            COL_HEADER_ROTATION = Alignment(text_rotation=90, horizontal='center', vertical='center')
            
        
        # Define a light red fill
        DATA_VALIDATION_ERROR_FILL = PatternFill(start_color="FFCCCB", end_color="FFCCCB", fill_type="solid")

        # Fetch column comments if config option is True
        INCLUDE_COMMENTS = current_app.config.get("INCLUDE_COLUMN_COMMENTS", False)
        
        # Start the offset at 0, add one 
        COMMENT_OFFSET = int(INCLUDE_COMMENTS)

        workbook = writer.book

        # Write each DataFrame to the appropriate sheet
        for sheetname, df in xls.items():
            df.to_excel(writer, sheet_name=sheetname, index=False, header = False)


        # Iterate over each sheet in the workbook
        for sheet in writer.sheets:
            # Access the active worksheet
            worksheet = writer.sheets[sheet]
            
            # Get the DataFrame corresponding to this sheet
            df = xls[sheet]
            
            if sheet in tabs_dict.keys():
                
                if INCLUDE_COMMENTS:
                    
                    # Grab the column comments from the database
                    column_comments = get_column_comments(sheet, eng)
                    comment_map = column_comments.set_index('column_name')['column_comment'].to_dict()

                    # Create a new DataFrame with comments as the first row
                    comment_row = [comment_map.get(col, '') for col in df.columns]
                    
                    worksheet.insert_rows(1)
                    
                    # Write the new row to the first row
                    for col_idx, value in enumerate(comment_row, start=1):
                        worksheet.cell(row=1, column=col_idx).value = value
                    
                # Write the column headers
                for col_idx, value in enumerate( list(df.columns), start = 1 ):
                    worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx).value = value
                
                
                
                
                tmp_pkey_cols = tabs_dict.get(sheet).get('pkey_fields', [])
                tmp_constrained_cols = tabs_dict.get(sheet).get('constrained_columns', [])
                tmp_fkey_details = tabs_dict.get(sheet).get('foreign_key_detail', {})
                
                # Create a dictionary to map column names to their 1-based indices
                col_indices = {col: idx + 1 for idx, col in enumerate(df.columns)}
                
                # Get the column indices for primary key fields (1-based index)
                pkey_bold_col_indices = [col_indices[col] for col in tmp_pkey_cols]
                non_pkey_col_indices = [col_indices[col] for col in list(set(df.columns) - set(tmp_pkey_cols))]
                
                # Get the column indices for foreign key fields (1-based index)
                fkey_highlighted_cols = [col_indices[col] for col in tmp_constrained_cols]
                
                # Apply formatting for primary key columns (bold font)
                for col_idx in pkey_bold_col_indices:
                    worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx).font = PKEY_BOLD_FONT
                
                # Apply formatting for NON primary key columns (NON bold font)
                for col_idx in non_pkey_col_indices:
                    worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx).font = Font(bold=False)
                
                # Apply formatting for foreign key columns (gray highlight)
                for col_idx in fkey_highlighted_cols:
                    worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx).fill = FKEY_HIGHLIGHT
                    
                    # Get the relevant foreign key details
                    column_name = df.columns[col_idx - 1]
                    foreign_key_info = tmp_fkey_details.get(sheet, dict()).get(column_name)
                    
                    if foreign_key_info is not None:
                    
                        # col_idx is the index for the excel sheet, which is a 1 based index
                        referenced_table = tmp_fkey_details.get(sheet).get(df.columns[ col_idx - 1 ]).get('referenced_table')
                        referenced_column = tmp_fkey_details.get(sheet).get(df.columns[ col_idx - 1 ]).get('referenced_column')
                        
                        # Add a comment to the header indicating the lookup table
                        header_cell = worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx)
                        comment_text = f"References {referenced_table}.{referenced_column}"
                        header_cell.comment = Comment(text=comment_text, author="System")
                        
                        
                        
                        referenced_sheetname = quote_sheetname(referenced_table)
                        referenced_sheet_column_letter = get_column_letter(xls.get(referenced_table).columns.get_loc(referenced_column) + 1)
                        
                        # Find the last row in the referenced table's worksheet
                        referenced_sheet = writer.sheets[referenced_table]
                        max_ref_row = referenced_sheet.max_row
                        
                        dv = DataValidation(
                            type="list",
                            formula1=f"={referenced_sheetname}!${referenced_sheet_column_letter}${2 + COMMENT_OFFSET}:${referenced_sheet_column_letter}${max_ref_row}",
                            allow_blank = True
                        )
                        
                        
                        dv.error ='Your entry is not in the list'
                        dv.errorTitle = 'Invalid Entry'
                        
                        
                        # Convert column index to Excel column letter
                        col_letter = get_column_letter(col_idx)
                        
                        # Apply the validation to the entire column, starting from row 2 + COMMENT_OFFSET
                        dv.add(f"{col_letter}{2 + COMMENT_OFFSET}:{col_letter}1048576")
                        
                        worksheet.add_data_validation(dv)
                        
                        # Apply Conditional Formatting to highlight invalid entries
                        formula = f'=AND({col_letter}{2 + COMMENT_OFFSET}<>"", COUNTIF({referenced_sheetname}!${referenced_sheet_column_letter}${2 + COMMENT_OFFSET}:${referenced_sheet_column_letter}${max_ref_row},{col_letter}{2 + COMMENT_OFFSET})=0)'

                        worksheet.conditional_formatting.add(
                            f"{col_letter}{2 + COMMENT_OFFSET}:{col_letter}1048576",
                            FormulaRule(formula=[formula], fill=DATA_VALIDATION_ERROR_FILL)
                        )


                # Apply rotation and centering to all column headers
                for col_idx in range(1, len(df.columns) + 1):  # Use 1-based indexing
                    worksheet.cell(row=1 + COMMENT_OFFSET, column=col_idx).alignment = COL_HEADER_ROTATION
            
            else:
                # Set the column widths based on max length in column
                for column_cells in worksheet.columns:
                    max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells[0:])
                    adjusted_width = max_length + 5  # Add cushion for a little extra space
                    worksheet.column_dimensions[column_cells[0].column_letter].width = adjusted_width

                # Apply filters to the specified header row
                if worksheet.max_row >= 0:  # Check if the header_row is within the data range
                    worksheet.auto_filter.ref = f"{worksheet.dimensions.split(':')[0]}:{worksheet.dimensions.split(':')[1]}"

    ############################################################################################################################
    ############################################################################################################################
    
    # set blob to the beginning
    excel_blob.seek(0)
    
    # Make a response object to set a custom cookie
    resp = make_response(send_file(excel_blob, as_attachment=True, download_name=f'{file_prefix}-TEMPLATE.xlsx'))

    # Set a cookie to let browser know that the file has been sent
    resp.set_cookie('template_file_sent', 'true', max_age=1)

    print("End Templater")

    return resp



