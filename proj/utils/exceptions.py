from .mail import send_mail
from flask import jsonify

def default_exception_handler(mail_from, errmsg, maintainers, project_name, login_info, submissionid, mail_server, attachment = None):
    print("Checker application came across an error")
    print(str(errmsg))
    response = jsonify(
        {
            'critical_error': True,
            'contact': ', '.join(maintainers)
        }
    )
    print("this is the response after the error message: ")
    print(response)
    print("response status code before: ")
    print(response.status_code)

    response.status_code = 500
    print("response status code after: ")
    print(response.status_code)
    # need to add code here to email SCCWRP staff about error

    msgbody = f"{project_name} Checker crashed.\n\n"
    msgbody += f"submissionid: {submissionid}\n"
    print("login_info:")
    print(login_info)
    if login_info is not None:
        print(login_info)
        for k, v in login_info.items():
            print(f"k: {k}")
            print(f"v: {v}")
            msgbody += f"{k}: {v}\n"
    msgbody += f"\nHere is the error message:\n{errmsg}"

    print("before send_mail function")
    send_mail(
        mail_from,
        maintainers,
        f"{project_name} Checker - Internal Server Error", 
        msgbody, 
        filename = attachment,
        server = mail_server
    )
    print("after send_mail function")
    return response
