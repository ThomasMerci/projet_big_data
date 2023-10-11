
docker network create projet_big_data_network
cd Docker_services
docker-compose up -d

timeout /t 30 >nul

cd ..

docker cp namenode:/etc/hadoop/core-site.xml ./core-site.xml
docker cp ./core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp NIFIFLOW.xml nifi:/opt/nifi/nifi-current/conf/NIFIFLOW.xml
docker cp NIFI.html nifi:/opt/nifi/nifi-current/conf/NIFI.html

timeout /t 30 >nul

docker-compose build
docker-compose up

