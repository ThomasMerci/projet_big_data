#!/bin/bash
cd Docker_services
docker-compose up -d

# Attendre 20 secondes
sleep 20

cd ..
docker cp namenode:/etc/hadoop/core-site.xml ./core-site.xml
docker cp ./core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker-compose build
docker-compose up
