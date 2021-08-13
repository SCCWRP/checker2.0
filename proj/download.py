import os
from flask import send_file, Blueprint, jsonify, request

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, attachment_filename = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")

@download.route('/export', methods = ['GET','POST'])
def template_file():
    filename = request.args.get('filename')

    if filename is not None:
        return send_file( os.path.join(os.getcwd(), "export", filename), as_attachment = True, attachment_filename = filename ) \
            if os.path.exists(os.path.join(os.getcwd(), "export", filename)) \
            else jsonify(message = "file not found")
    else:
        return jsonify(message = "filename not provided")