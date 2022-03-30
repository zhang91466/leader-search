#!/bin/sh

main_folder='/databases/gpdata/data1/docker/postgresql-plus'
data_folder=$main_folder/data

cd $main_folder

mkdir -p $data_folder

docker build -f Dockerfile -m 4g -t postgresql-plus:v1 .

docker build -f Dockerfile.master -m 4g -t l_search:v1 .

docker run \
    --name postgresql-postgis \
    -e POSTGRES_PASSWORD=123456xxx \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    -v $data_folder:/var/lib/postgresql/data \
    -p 6688:5432 \
    -d postgresql-plus:v1

docker-compose --compatibility up -d


docker run \
    --name l_search_run \
    -p 6680:5000 \
    -d l_search:v1

docker-compose build --force-rm

