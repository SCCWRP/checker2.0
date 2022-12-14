import re, os, glob, csv
import pandas as pd
from flask import g, current_app

def reformat(submission_dir, filename, login_info):
    print("Reformat routine")
    projectid = login_info.get('projectid')
    siteid = login_info.get('siteid')
    estuaryname = pd.read_sql(f'select distinct estuary from lu_siteid where siteid = {siteid} limit 1', g.eng)
    #estuaryname = login_info.get('estuaryname') #####
    stationno = login_info.get('stationno')
    sensortype = login_info.get('sensortype')
    wqnotes = login_info.get('notes')
    oldcsvpath = os.path.join(submission_dir, filename)
    new_excel_path = f'{oldcsvpath.rsplit(".", 1)[0]}-reformatted.xlsx'

    if sensortype.lower().strip() == 'tidbit':
        headers = ['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestamp', 'h2otemp_c', 'wqnotes']
        # this csv has a different encoding, which is weird, may cause an issue?
        # just read first line to get serial number
        with open(oldcsvpath, encoding='utf-8-sig') as tidbit_file:
            # split on ':' so easy to access serial number
            # this file uses quotes to prevent splitting, so we ignore quotes while reading, and strip them out later
            tidbit_reader = csv.reader(tidbit_file, delimiter = ':', quoting = csv.QUOTE_NONE) 
            tidbit_serial_num = tidbit_reader.__next__()[1].strip(' "')
        # use only 2nd and 3rd column since those are datetimes and temperature readings
        tidbit_data = pd.read_csv(oldcsvpath, encoding='utf-8-sig', header = 1, usecols = [1,2], parse_dates = [0], infer_datetime_format = True)
        # this one has extra columns that I don't think we care about, which introduces some NaNs between the regular intervals
        tidbit_data = tidbit_data[tidbit_data.iloc[:,1].isna() == False]

        tidbit_data['projectid'] = projectid
        tidbit_data['siteid'] = siteid
        tidbit_data['estuaryname'] = estuaryname
        tidbit_data['stationno'] = stationno
        tidbit_data['sensortype'] = sensortype
        tidbit_data['sensorid'] = tidbit_serial_num
        tidbit_data['samplecollectiontimestamp'] = tidbit_data.iloc[:,0].dt.tz_localize('US/Pacific').dt.tz_convert('UTC')
        tidbit_data['h2otemp_c'] = tidbit_data.iloc[:,1]
        tidbit_data['wqnotes'] = wqnotes
        print(tidbit_data)
        
        with pd.ExcelWriter(new_excel_path) as writer:
            tidbit_data[headers].to_excel(writer, index = False)
        
    
    elif sensortype.lower().strip() == 'minidot':
        headers = ['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestamp', 'h2otemp_c', 'do_mgl', 'do_percent', 'qvalue', 'wqnotes']
        # read csv header, get serial number/sensor ID from first two lines of header
        with open(oldcsvpath) as minidot_file:
            minidot_reader = csv.reader(minidot_file, delimiter = ':') # splitting on ':' puts sensor ID in standard position on second line
            minidot_reader.__next__() # read first line, do nothing with it
            minidot_serial_num = minidot_reader.__next__()[1].strip() # read second line, get second item in list, which is sensor ID, strip out any whitespace

        # read in minidot data, starting at row 5
        # tab separated, but can sometimes be more than one tab
        # since regex, this uses python engine to parse through csv, which may be slow
        # tested, 3 times slower than c engine, but files won't be large enough for this to matter, I think
        minidot_data = pd.read_csv(oldcsvpath, header = 5, usecols = [2,3,4,5,6,7], sep = '\t+', engine = 'python')
        minidot_data.drop(0, inplace = True) # drop first row, which is just units for each column
        minidot_data.iloc[:,0] = pd.to_datetime(minidot_data.iloc[:,0])
        minidot_data.iloc[:,1:] = minidot_data.iloc[:,1:].astype('float64')

    elif sensortype.lower().strip() == 'ctd':
        headers = ['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestamp', 'pressure_mh2o', 'h2otemp_c', 'conductivity_sm', 'salinity_ppt', 'wqnotes']
        # this data is already in a friendly format :D
        ctd_data = pd.read_excel(oldcsvpath)

    elif sensortype.lower().strip() == 'troll':
        headers = ['projectid', 'siteid', 'estuaryname', 'stationno', 'sensortype', 'sensorid', 'samplecollectiontimestamp', 'pressure_mh2o', 'h2otemp_c', 'depth_m', 'wqnotes']
        # read csv header, get values for reading in as dataframe later
        with open(oldcsvpath) as troll_file:
            troll_reader = csv.reader(troll_file)
            num_blank_lines = 0 # number of blank lines for header_index calc below
            for i, row in enumerate(troll_reader):
                if len(row) == 0:
                    num_blank_lines += 1 
                elif len(row) > 0 and 'Serial Number' in row:
                    serial_index = row.index('Serial Number')
                    troll_serial_num = row[serial_index + 1] # text search for serial number
                elif len(row) > 0 and 'Time Zone:' in row[0]: # 'Time Zone:' indicates data is about to start
                    header_index = i + 3 - num_blank_lines # data begins 4 rows below 'Time Zone:' line, adjust for blank lines and 0 indexing
                elif len(row) > 0 and 'Elapsed Time' in row: # if get to end of header, don't read rest of file
                    break

        troll_data = pd.read_csv(oldcsvpath, header = header_index, usecols = [0,2,3,4])
        troll_data['projectid'] = projectid
        troll_data['siteid'] = siteid
        troll_data['estuaryname'] = estuaryname
        troll_data['stationno'] = stationno
        troll_data['sensortype'] = sensortype        
        troll_data['sensorid'] = troll_serial_num
        troll_data['samplecollectiontimestamp'] = pd.to_datetime(troll_data.iloc[:,0])
        troll_data['pressure_mh2o'] = troll_data.iloc[:,1]
        troll_data['h2otemp_c'] = troll_data.iloc[:,2]
        troll_data['depth_m'] = troll_data.iloc[:,3]

    else:
        pass
    
    newfilename = f"{filename.rsplit('.', 1)[0]}-reformatted.xlsx"

    return new_excel_path, newfilename

# def reformat_csv(submission_dir, filename):
#     oldcsvpath = os.path.join(submission_dir, filename)
#     with open(oldcsvpath, "r") as logFile:
#         print("Reformat routine")
#         print("Reading content")
#         content = logFile.readlines()

#         print("Reading header")
#         header = content[1] # expecting the header to be at row 2
#         print("splitting header at commas")
#         columns = header.split(",\"") 

#         print("finding pendant ID")
#         print('content')
#         print(re.findall(r'\d+', content[0]))
#         pendantId = re.findall(r'\d+', content[0])[0]

#         #appendRow = ""
#         tempUnit = "F"
#         intensityUnit = "lum/ft²"

#         print("Looping through columns")
#         newcols = []
#         for c in columns:
#             try:
#                 print(c)
#             except Exception:
#                 print(c.encode('utf-8'))
#             if "#" in c:
#                 newcols.append("rec_num")
#                 #appendRow += "rec_num,"
#             elif "Date Time" in c:
#                 newcols.append("datetime")
#                 #appendRow += "datetime,"
#             elif "Temp" in c:
#                 newcols.append("temperature")
#                 #appendRow += "temperature,"
#                 print("# Extract the temperature unit from the column name")
#                 tempUnit = "Deg{}".format(re.findall(r'°(\w+)', c)[0]) # Extract the temperature unit from the column name
#             elif "Intensity" in c:
#                 newcols.append("intensity")
#                 #appendRow += "intensity,"
#                 print("# Extract the intensity from the column name")
#                 intensityUnit = re.findall(r'(?<=,)(.*?)(?=\()', c)[0].strip()
#             else:
#                 # just need to check first if the column name matched the format we expected
#                 tmp = re.findall(r'(.*)\(.*\)', c)
#                 print('tmp')
#                 print(tmp)
#                 if len(tmp) > 0:
#                     print('replace whitespace with underscores')
#                     newcols.append(re.sub('\s+', '_', tmp[0].strip().lower().replace(',','')))
#                     #appendRow += re.sub('\s+', '_', colname[0].strip().lower())
#                     #appendRow += ','
#                 else:
#                     print("Column name seems to not be what we expected. Here it is:\n")
#                     print(c)
            

#         # Grab extra columns that are in the table, but weren't in the submitted csv
#         # Rafi will want us to indicate whether a column was added by us, due to them not having the column, 
#         #  versus them giving us the column but not putting any data in it
#         othercols = list(
#             set(pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'tbl_data_logger_raw';", g.eng).column_name.tolist()) 
#             - 
#             set(newcols)
#             -
#             set(current_app.system_fields)
#         )

#         newcols.extend(othercols)
#         #newcols.extend(['temperature_units','intensity_units','pendantid'])

#         appendRow = ','.join(newcols)
#         appendRow += '\n'

#         # add column description headers
#         print("# add column description headers")
#         content[1] = content[1].replace("\n", "") + ',' + ",".join(othercols) + "\n" #+ ",Temperature Units,Intensity Units,Pendant Id\n"
        
#         print("Append rows")
#         content.insert(2, appendRow)

#         tempindex = newcols.index('temperature_units')
#         intensityindex = newcols.index('intensity_units')
#         pendantindex = newcols.index('pendantid')
#         missing_col_indices = [newcols.index(x) for x in othercols if x not in ('temperature_units','intensity_units','pendantid')]
#         for i in range(len(content[3:])):
#             #content[i + 3] = content[i + 3].replace("\n", "") + "," + tempUnit + "," + intensityUnit + "," + pendantId + "\n"
#             tmp = [x.strip() for x in content[i + 3].split(',')]
#             tmp += [''] * len(othercols)
#             tmp[tempindex] = tempUnit
#             tmp[intensityindex] = intensityUnit
#             tmp[pendantindex] = pendantId
#             for j in missing_col_indices:
#                 tmp[j] = 'column was not provided'

#             try:
#                 print("content[i + 3] = f{','.join(tmp)")
#                 content[i + 3] = f"{','.join(tmp)}\n"
#             except Exception as e:
#                 print(e)
#                 print(content)
#                 raise Exception
            
            
#     newcsvpath = os.path.join(submission_dir, f"{filename.rsplit('.', 1)[0]}-reformatted.csv")
#     with open(newcsvpath, "w") as f:
#         contents = "".join(content)
#         f.write(contents)

#     new_excel_path = f"{newcsvpath[:-4]}.xlsx"
#     workbook = Workbook(f"{newcsvpath[:-4]}.xlsx")
#     worksheet = workbook.add_worksheet()
#     with open(newcsvpath, 'rt', encoding='utf8') as f:
#         reader = csv.reader(f)
#         for r, row in enumerate(reader):
#             for c, col in enumerate(row):
#                 worksheet.write(r, c, col)
#     workbook.close()

#     print('new file name')
#     newfilename = f"{filename.rsplit('.', 1)[0]}-reformatted.xlsx"
#     print(newfilename)
#     return new_excel_path, newfilename