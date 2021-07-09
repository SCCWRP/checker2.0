from json import dump
from pandas import DataFrame
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from email import encoders
import smtplib


# a function used to collect the warnings to store in database
# in other words, if a user submitted flagged data we want to store that information somewhere in the database to make it easier to hunt down flagged data
def collect_error_messages(errs):
    """
    errs is the list of errors or warnings that were collected during the checking process
    offset is the number of rows the excel files are offsetted, based on how the people set up their data submission templates
    """

    assert isinstance(errs, list), "function - collect_warnings - errs object is not a list"
    if errs == []:
        return []
    assert all([isinstance(x, dict) for x in errs]), "function - collect_warnings - errs list contains non-dictionary objects"
    assert all(["columns" in x.keys() for x in errs]), "function - collect_warnings - 'columns' not found in keys of a dictionary in the errs list"
    assert all(["rows" in x.keys() for x in errs]), "function - collect_warnings - 'rows' not found in keys of a dictionary in the errs list"
    assert all(["table" in x.keys() for x in errs]), "function - collect_warnings - 'table' not found in keys of a dictionary in the errs list"
    assert all(["error_message" in x.keys() for x in errs]), "function - collect_warnings - 'error_message' not found in keys of a dictionary in the errs list"


    output = [
        {
            # This will be written to a json and stored in the submission directory
            # to be read in later during the final submission routine, 
            # or in the routine which marks up their excel file
            "columns"         : w['columns'],
            "table"           : w['table'],
            "row_number"      : r['row_number'],
            "message"         : f"{w['columns']} - {w['error_message']}"
        }
        for w in errs
        for r in w['rows']
    ]

    output = DataFrame(output).groupby(['row_number', 'columns', 'table']) \
        .apply(
            # .tolist() doesnt work. 
            lambda x: ';'.join( list(x['message']) ) 
        ).to_dict() 

    
    return [{'row_number': k[0], 'columns': k[1], 'table': k[2], 'message': v} for k, v in output.items()]



def correct_row_offset(lst, offset):
    # By default the error and warnings collection methods assume that no rows were skipped in reading in of excel file.
    # It adds 1 to the row number when getting the error/warning, since excel is 1 based but the python dataframe indexing is zero based.
    # Therefore the row number in the errors and warnings will only match with their excel file's row if the column headers are actually in 
    #   the first row of the excel file.
    # These next few lines of code should correct that

    [
        r.update(
            {
                'message'     : r['message'],
                
                # to get the actual excel file row number, we must add the number of rows that pandas skipped while first reading in the dataframe,
                #   and we must add another row to account for the row in the excel file that contains the column headers 
                #   and another 1 to account for the 1 based indexing of excel vs the zero based indexing of python
                'row_number'  : r['row_number'] + offset + 1 + 1 ,
                'value'       : r['value']
            }
        )
        for e in lst
        for r in e['rows']
    ]


    return lst




def save_errors(errs, filepath):
    errors_file = open(filepath, 'w')
    dump(
        collect_error_messages(errs),
        errors_file
    )
    errors_file.close()



# Function to be used later in sending email
def send_mail(send_from, send_to, subject, text, filename=None, server="localhost"):
    msg = MIMEMultipart()
    
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject
    
    msg_content = MIMEText(text)
    msg.attach(msg_content)
    
    if filename is not None:
        attachment = open(filename,"rb")
        p = MIMEBase('application','octet-stream')
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition','attachment; filename= %s' % filename.split("/")[-1])
        msg.attach(p)

    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()