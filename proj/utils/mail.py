from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import COMMASPACE, formatdate
from email import encoders
import smtplib
import pandas as pd

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


def data_receipt(send_from, always_send_to, login_email, dtype, submissionid, originalfile, tables, eng, mailserver, login_info, *args, **kwargs):
    """
    Depending on the project, this function will likely need to be modified. In some cases there are agencies and data owners that
    must be incuded in the email body or subject.

    send_from must be a string, like admin@chceker.sccwrp.org
    always_send_to are the email addresses which will always receive the email, which may or may not always be the maintainers
    login_email is the email the user logged in with
    dtype is the data type they submitted data for
    submissionid is self explanatory
    tables is the list of all tables they submitted data to
    eng is the database connection to confirm the records were loaded
    mailserver is the server that will be used to send the email
    """

    email_subject = f"Successful Data Load - {dtype} -- Submission ID#: {submissionid}"
    email_body = f"SCCWRP has received a successful {dtype} data submission from {login_email}\n"
    email_body += f"Submission ID: {submissionid}\n"
    email_body += f"Date Received: {pd.Timestamp(submissionid, unit = 's').strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    email_body += "Session Login Information:\n\t"
    for k,v in login_info.items():
        email_body += f"{k}: {v}\n\t"
    email_body += "\n\n"
    email_body += "\n".join(
        [
            f"{pd.read_sql(f'SELECT COUNT(*) AS n_records FROM {tbl} WHERE submissionid = {submissionid};', eng).n_records.values[0]} records loaded to {tbl}" 
            for tbl in tables 
        ]
    )

    send_mail(send_from, [*always_send_to, login_email], email_subject, email_body, filename = originalfile, server = mailserver)



