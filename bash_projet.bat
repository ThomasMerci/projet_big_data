
docker-compose build
docker-compose up


docker cp nifi:/opt/nifi/nifi-current/conf/data_bikes.csv data/data_bikes.csv 
docker cp nifi:/opt/nifi/nifi-current/conf/df_ml.csv data/df_ml.csv