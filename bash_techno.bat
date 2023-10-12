
docker network create projet_big_data_network
cd Docker_services
docker-compose up -d

timeout /t 10 >nul

cd ..

docker cp namenode:/etc/hadoop/core-site.xml ./core-site.xml
docker cp ./core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp NIFIFLOW.xml nifi:/opt/nifi/nifi-current/conf/NIFIFLOW.xml
docker cp NIFI.html nifi:/opt/nifi/nifi-current/conf/NIFI.html
docker cp NIFIFLOW.xml nifi:/opt/nifi/nifi-current/conf/archive/NIFIFLOW.xml

docker cp Python/texte.csv nifi:/opt/nifi/nifi-current/conf/texte.csv
docker cp data/bikes.csv nifi:/opt/nifi/nifi-current/conf/bikes.csv
docker cp data/bikeshops.csv nifi:/opt/nifi/nifi-current/conf/bikeshops.csv
docker cp data/orders.csv nifi:/opt/nifi/nifi-current/conf/orders.csv
docker cp data/customers.csv nifi:/opt/nifi/nifi-current/conf/customers.csv
