from .mail import send_mail
from flask import jsonify

def default_exception_handler(mail_from, errmsg, maintainers, project_name, login_info, submissionid, mail_server, attachment = None):
    print("Checker application came across an error")
    print(errmsg) # commenting out due to unicodeencodeerror
    response = jsonify(
        {
            'critical_error': True,
            'contact': ', '.join(maintainers)
        }
    )
    response.status_code = 500
    # need to add code here to email SCCWRP staff about error

    msgbody = f"{project_name} Checker crashed.\n\n"
    msgbody += f"submissionid: {submissionid}\n"
    if login_info is not None:
        for k, v in login_info.items():
            msgbody += f"{k}: {v}\n"
    msgbody += f"\nHere is the error message:\n{errmsg}"

    send_mail(
        mail_from,
        maintainers,
        f"{project_name} Checker - Internal Server Error", 
        msgbody, 
        filename = attachment,
        server = mail_server
    )
    return response
