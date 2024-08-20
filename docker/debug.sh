#!/bin/bash
submissionid=$1
sudo docker container exec -it -w /var/www/checker/files/$submissionid bight23-checker ipython;