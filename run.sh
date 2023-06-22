#!/bin/bash

## Pull Docker images
#docker-compose pull
#
## Build Docker containers
#docker-compose build
#
## Run the databases containers
#docker-compose up -d
#
## Run the venv
#./venv/Scripts/activate.bat
#
#pip install -r requirements.txt > /dev/null 2>&1
#
## Runs tests
#python /test_directory/test_join_pipelines.py
#
## Run Python script
#python sqlitedb.py
#python redisdb.py

python join_methods.py dataset100k hash_join


