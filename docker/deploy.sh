#!/bin/bash
# Arg #1 is the container and folder name
# Arg #2 is the Database connection string
docker container run -it -d --name $1 \
    -v /tmp:/tmp -v /var/www/templates/$1:/var/www/$1 \
    -v /etc/timezone:/etc/timezone:ro \
    -v /etc/localtime:/etc/localtime:ro \
    -w /var/www/checker \
    -e DB_CONNECTION_STRING=$2 \
    sccwrp/flask:checkertemplate \
    uwsgi -s /tmp/$1.sock --uid www-data --gid www-data --manage-script-name --mount /checker=run:app --chmod-socket=666

# docker container run -it -d \
#     --name $1 \
#     -e DB_CONNECTION_STRING=$2 \
#     -e FLASK_APP_SECRET_KEY=$3 \
#     -e FLASK_APP_MAIL_SERVER=$4 \
#     -e FLASK_APP_MAIL_FROM=$5 \
#     -e FLASK_APP_EXCEL_OFFSET=$6 \
#     -e FLASK_APP_SCRIPT_ROOT=$7 \
#     -e PROJNAME=$8 \
#     -v {checker_dir}:{checker_docker_dir} \
#     -v /tmp:/tmp \
#     -v /etc/timezone:/etc/timezone:ro \
#     -v /etc/localtime:/etc/localtime:ro \
#     -w {checker_docker_dir} \
#     checkerimage \
#     uwsgi -s /tmp/{checker_scriptroot}.sock --uid www-data --gid www-data --manage-script-name --mount /{checker_scriptroot}=run:app --chmod-socket=666

