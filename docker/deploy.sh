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
    uwsgi -s /tmp/$1.sock --uid www-data --gid www-data --manage-script-name --buffer-size 65535 --mount /checker=run:app --chmod-socket=666
