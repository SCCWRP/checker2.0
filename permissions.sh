#!/bin/bash
if [ -z "$1" ]
    then 
        editor="admin"
else
    editor=$1
fi

DIRECTORY=$(cd `dirname $0` && pwd)
chown -R $editor.$editor $DIRECTORY;
chown -R www-data.$editor $DIRECTORY/files $DIRECTORY/proj/custom $DIRECTORY/proj/static $DIRECTORY/proj/config;
chmod -R 775 $DIRECTORY/files $DIRECTORY/proj/custom $DIRECTORY/proj/static;
chmod -R 575 $DIRECTORY/proj/config;