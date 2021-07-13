from .mail import send_mail
from flask import jsonify

def default_exception_handler(mail_from, errmsg, maintainers, project_name, mail_server):
    print("Checker application came across an error")
    print(errmsg)
    response = jsonify(
        {
            'critical_error': True,
            'contact': ', '.join(maintainers)
        }
    )
    response.status_code = 500
    # need to add code here to email SCCWRP staff about error
    send_mail(
        mail_from,
        maintainers,
        f"{project_name} Checker - Internal Server Error", 
        f"{project_name} Checker crashed. Here is the error message:\n{errmsg}", 
        server = mail_server
    )
    return response