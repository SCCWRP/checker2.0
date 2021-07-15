FROM debian:latest
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update 
RUN apt-get install -y apt-utils
RUN apt-get install -y python3-pip python3-dev build-essential libssl-dev libffi-dev
RUN apt-get install -y libpcre3 libpcre3-dev
RUN apt-get install -y vim
RUN pip3 install wheel
RUN pip3 install uwsgi flask ipython
RUN pip3 install xlrd flask_session
RUN pip3 install flask_cors
RUN pip3 install sqlalchemy
RUN apt-get install -y libpq-dev && pip3 install psycopg2
RUN pip3 install pandas xlsxwriter numpy
RUN pip3 install folium
RUN pip3 install pandas_access
RUN pip3 install flask_mail
RUN pip3 install geoalchemy2
RUN pip3 install odo
RUN pip3 install cerberus
RUN pip3 install Werkzeug
RUN pip3 install openpyxl
#RUN pip3 install opencv-python

RUN mkdir -p /var/www/checker/
WORKDIR /var/www/checker/

CMD ["uwsgi","-s","/tmp/nese.sock","--uid","www-data","--gid","www-data","--manage-script-name","--mount","/nese=run:app","--chmod-socket=666"]


# docker container run -it -d --name appname \
#     -v /tmp:/tmp -v /var/www/templates/appname:/var/www/appname \
#     -v /etc/timezone:/etc/timezone:ro \
#     -v /etc/localtime:/etc/localtime:ro \
#     -e DB_CONNECTION_STRING='db_connection_string whatever it is' \
#     -e FLASK_APP_SECRET_KEY='some secret key' \
#     sccwrp/flaskchecker:latest
