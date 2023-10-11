cd Docker_services
docker-compose up -d

timeout /t 20 >nul

cd ..

docker cp namenode:/etc/hadoop/core-site.xml ./core-site.xml
docker cp ./core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp NIFIFLOW.xml nifi:/opt/nifi/nifi-current/conf/NIFIFLOW.xml

docker-compose build
docker-compose up

