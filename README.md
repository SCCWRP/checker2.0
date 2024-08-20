# Data Checker

This checker application is basically a refactoring of the original checker application developed by SCCWRP for Bight 2018 and the SMC regional monitoring programs.  
(That repository is private)   

This application is designed mainly to shorten the QA/QC processes which always come with collection of environmental data.

This repository is more or less a template to set up checker applications for different projects. 

## How it works
### Database
When a data checking application is created, it is associated with a PostgreSQL geodatabase. Specifically they are ESRI's enterprise geodatabases. Those databases are created with SCCWRP's own jupyter notebooks on our own Arc GIS portal server (gis.sccwrp.org). These notebooks are not publicly accessible, nor can we publish them, but they are created with the ArcPy library which is a non-open source ESRI product. 

This solution of using the checker application with an ESRI enterprise geodatabase is not necessarily the only option, as there are likely other ways we may have implemented this applciation completely with only open source projects, such as QGIS. We have not explored this route nor have we had a need to, but it would be possible to fork this repository and adapt the code to work with that kind of a database. Depending on the type of data being collected, it may not even be of any advantage to use a geodatabase at all.  

Many of our databases are hosted with Amazon RDS, or locally behind our own firewall

### Checking System
Data submissions are divided into categories which we call data types or data sets. These are named groupings of tables in the database. A typical example would be that chemistry data (lab results) and the associated meta data need to be submitted together, so the two tables would be grouped and called "chemistry"   

Template files are provided to users for them to fill data in with. These template files provided to the user must match the column headers of the database tables. The first thing the checker does is check to make sure the column headers match with tables in the database, and after that, it makes sure that the tabs in the excel file match at least one of the grouping of tables provided in the "DATASETS" in the app's configuration. 

For this process to work, it is important that the tables in the database follow the naming convention that all tables which are intended to receive data from the checker application are prefixed with "tbl_"  
(We can most likely adjust it so anyone who deploys the app can adjust the prefix as well, but there is no need for us to focus on that at this moment)

Template files are provided to users for them to fill data in with. These template files provided to the user must match the column headers of the database tables. The first thing the checker does is check to make sure the column headers match with tables in the database, and after that, it makes sure that the tabs in the excel file match at least one of the grouping of tables provided in the "DATASETS" in the app's configuration. For this process to work, it is important that the tables in the database follow the naming convention.

After matching the tables to a dataset, it then checks the data in the tables against the schema in the database. It checks primary keys, foregin keys, datatypes of columns, as well as length and precision of columns. The application refers to these QA/QC checks as "Core Checks" as these are the most basic requirements the data must meet. If it does not meet the requirements and rules laid out by the database schema, then the data could not even go into the tables even if we tried to insert the data without the checker app.  

Foreign key "Core Checks" are essentially "lookup list" checks. To restrict values that can go into a column, we create lookup lists in the database, prefix the table names with "lu_" and make the primary key column the one which contains the values we want to restrict the user to. We then place a foreign key constraint on the column for which we want to restrict the values, referencing the primary key column of the "lu_" table. 

If the data meets these core requirements, the data then enters the stage of "Custom Checks" which are additional QA/QC checks provided by a scientist, project manager, or anyone else with authority to decide additional requirements for data acceptance.

If the data meets these requirements, then the user will be prompted with a "Final Submit" button, which will insert their data into their respective tables in the database, and they will receive a confirmation email of their submission.

### Additional info
The application is deployed using docker, and the Dockerfile is included in the repository. Within the Dockerfile, there is an example of a deployment script at the bottom, commented out. There are certain environment variables that need to be in the os environment in order for the app to run correctly, such as the database connection information, and the flask app secret key.

This application runs on the Flask microframework, which is a web application framework for python. We use the pandas library to read in the user's data and check the dataframes, and to load the data to the tables. The database connection in the application is a SQLAlchemy Engine object, which gets used by pandas.

Contributors:
- Paul Smith (pauls@sccwrp.org)
- Robert Butler (robertb@sccwrp.org)
- Duy Nguyen (duyn@sccwrp.org)
- Kristen
- Jordan Golemo
- Eric Hermoso
- Zaib Quraishi (zaibq@sccwrp.org)
- Matthew McCauley
