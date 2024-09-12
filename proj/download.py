import os
from flask import send_file, Blueprint, jsonify

download = Blueprint('download', __name__)
@download.route('/download/<submissionid>/<filename>', methods = ['GET','POST'])
def submission_file(submissionid, filename):
    return send_file( os.path.join(os.getcwd(), "files", submissionid, filename), as_attachment = True, download_name = filename ) \
        if os.path.exists(os.path.join(os.getcwd(), "files", submissionid, filename)) \
        else jsonify(message = "file not found")


