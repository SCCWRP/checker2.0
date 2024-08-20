# IMAGE NAME sccwrp/flask:bight23checker
FROM debian
FROM debian
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update 
RUN apt-get install -y apt-utils
RUN apt-get install -y python3-pip python3-dev build-essential libssl-dev libffi-dev
RUN apt-get install -y libpcre3 libpcre3-dev
RUN apt-get install -y vim
# install postgres to have psql command line utility
RUN apt-get install -y postgresql postgresql-contrib
RUN apt-get install -y libpq-dev
RUN apt-get install -y libgdal-dev
RUN apt-get install -y libproj-dev
RUN apt-get install -y cython3
RUN apt-get install -y python3-pyproj

RUN pip3 install wheel
RUN pip3 install psycopg2
RUN pip3 install uwsgi flask ipython
RUN pip3 install xlrd flask_session
RUN pip3 install flask_cors
RUN pip3 install Werkzeug
RUN pip3 install "pandas<2.0.0" 
RUN pip3 install "sqlalchemy<2.0.0"
RUN pip3 install xlsxwriter
RUN pip3 install openpyxl
RUN pip3 install numpy 
RUN pip3 install postgis
ENV PROJ_DIR=/usr/
RUN pip3 install fiona
RUN pip3 install shapely
ENV PROJ_LIBDIR=/usr/lib
ENV PROJ_INCDIR=/usr/lib
RUN pip3 install geopandas 
RUN pip3 install folium 
RUN pip3 install matplotlib 
RUN apt-get install -y heimdal-dev
RUN pip3 install arcgis 
RUN pip3 install scipy 
RUN pip3 install shapely 
#RUN pip3 install opencv-python
# Install what we need for the R Python bridge
RUN apt-get install -y r-base
WORKDIR /tmp

# Install dependencies for the necessary R packages

# readxl and openxlsx will always be needed as an interface for excel files
RUN Rscript -e 'install.packages(c("readxl"))'
RUN Rscript -e 'install.packages(c("openxlsx"))'

# dependencies for SQOUnified
RUN Rscript -e 'install.packages(c("DBI", "dbplyr", "reshape2", "RPostgreSQL"))'
RUN Rscript -e 'install.packages(c("rstudioapi", "vegan", "dplyr", "plyr", "purrr", "stringr", "tidyr"))'

# dependencies for phcorrection
RUN Rscript -e 'install.packages(c("seacarb"))'

# install SQOUnified
# You will need to go to github and generate a personal access token with repo access, and use it as a build arg
# https://github.com/settings/tokens
RUN apt-get install -y git
ARG access_token='default'
ENV access_token_=$access_token

RUN git clone "https://r7butler:${access_token_}@github.com/SCCWRP/SQOUnified"
RUN R CMD INSTALL --no-multiarch --with-keep.source SQOUnified

# install phcorrection
RUN git clone https://github.com/SCCWRP/phcorrection
RUN R CMD INSTALL --no-multiarch --with-keep.source phcorrection

RUN mkdir -p /var/www/checker/
#WORKDIR /var/www/checker/

RUN pip3 install geopy
RUN pip3 install shareplum



#CMD ["uwsgi","-s","/tmp/empachecker.sock","--uid","www-data","--gid","www-data","--manage-script-name","--mount","/checker=run:app","--chmod-socket=666"]
# docker container run -it -d --name $1 \
#     -v /tmp:/tmp -v /var/www/templates/$1:/var/www/$1 \
#     -v /etc/timezone:/etc/timezone:ro \
#     -v /etc/localtime:/etc/localtime:ro \
#     -w /var/www/checker \
#     -e DB_CONNECTION_STRING='$2' \
#     -e FLASK_APP_SECRET_KEY=$3 \      
#     sccwrp/flask:checkertemplate \
#     uwsgi -s /tmp/$1.sock --uid www-data --gid www-data --manage-script-name --mount /checker=run:app --chmod-socket=666
