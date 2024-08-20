import os
import pandas as pd

from flask import Blueprint, g, request, render_template, flash, redirect, url_for

from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
from io import BytesIO

info = Blueprint('info', __name__)

teams_username = os.environ.get("MS_USERNAME")
teams_password = os.environ.get("MS_PASSWORD")
sitefolder = os.environ.get("TEAMS_SITEFOLDER")
url = os.environ.get("SHAREPOINT_SITE_URL")
teamname = 'Bight2023IM'
instructions_filename = os.environ.get('INSTRUCTIONS_FILENAME')



# attnpts = attention points
@info.route('/info', strict_slashes = False)
def attnpts():
    datatype = request.args.get('dtype')

    # Get new authcookie upon request
    authcookie = Office365(url, username=teams_username, password=teams_password).GetCookies()
    site = Site(os.path.join(url, 'sites', teamname), version=Version.v2016, authcookie=authcookie)
    folder = site.Folder(sitefolder)
        
    comments_ = folder.get_file(instructions_filename)
    commentbytes = BytesIO(comments_)
    
    # Load the Excel file without specifying a sheet
    xls = pd.ExcelFile(commentbytes)
    sheet_names = xls.sheet_names
    
    if datatype is None:
        return render_template('attention_points.jinja2', datatypes_list = sheet_names)
    
    # due to my paranoia of things not being the datatype that i expect
    # as far as im concerned the thing should be a NoneType or a string
    # if the code got this far, its not a NoneType
    datatype = str(datatype)

    if datatype.lower() not in [str(sn).lower() for sn in xls.sheet_names]: # sn = sheet name
        flash(f"datatype {datatype} not found")
        return render_template('attention_points.jinja2', datatypes_list = sheet_names)
    
    # safe to assume this list will be non-empty
    datatype = [dtyp for dtyp in sheet_names if str(dtyp).lower() == datatype.lower()][0]

    comments = pd.read_excel( xls, sheet_name = datatype )
    comments.columns = [c.lower() for c in comments.columns]

    if "comments" not in comments.columns:
        flash(f"There is an error that SCCWRP staff must fix - Comments column not found (in {datatype} instructions)")
        return render_template('attention_points.jinja2', datatypes_list = sheet_names)

    attention_points = comments.comments.tolist()
    
    return render_template('attention_points.jinja2', datatype = datatype, attention_points=attention_points)



