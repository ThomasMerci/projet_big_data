#!/bin/bash
cd Docker_services
docker-compose up -d

timeout /t 30 >nul

cd ..

docker cp namenode:/etc/hadoop/core-site.xml ./core-site.xml
docker cp ./core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml

docker-compose build
docker-compose up

