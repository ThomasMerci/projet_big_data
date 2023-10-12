
Filter changed files
  24 changes: 17 additions & 7 deletions24  
Docker_services/docker-compose.yml
@@ -144,28 +144,38 @@ services:
      - projet_big_data_network

  prometheus:
    image: bitnami/prometheus:2.47.0
    image: bitnami/prometheus
    container_name: prometheus
    restart: always
    ports:
      - 9090:9090
    volumes:
      - prometheus_data:/bitnami/prometheus
    env_file:
      - ./prometheus.env
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    networks:
      - projet_big_data_network

  pushgateway:
    image: bitnami/pushgateway
    container_name: pushgateway
    ports:
      - "9091:9091"
    networks:
      - projet_big_data_network

  grafana:
    image: grafana/grafana:9.4.14
    image: grafana/grafana
    container_name: grafana
    restart: always
    ports:
      - 3000:3000
    volumes:
      - grafana_data:/var/lib/grafana
    env_file:
      - ./grafana.env
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      PROMETHEUS_URL: "http://prometheus:9090"
    depends_on:
      - prometheus
    networks:
      - projet_big_data_network

@@ -245,4 +255,4 @@ volumes:

networks:
  projet_big_data_network:
    external: true
    external: true
 11 changes: 0 additions & 11 deletions11  
Docker_services/prometheus.env
This file was deleted.

 15 changes: 15 additions & 0 deletions15  
Docker_services/prometheus.yml
@@ -0,0 +1,15 @@
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['prometheus:9090']  

  - job_name: 'pushgateway'
    static_configs:
      - targets: ['pushgateway:9091'] 

  - job_name: 'python'
    static_configs:
      - targets: ['projet_big_data_python:8004']
  32 changes: 32 additions & 0 deletions32  
Python/python_deltalake.py
@@ -9,6 +9,9 @@
from delta import *
import pandas as pd
from delta import DeltaTable
from prometheus_client import *
import time
import psutil
import python_extract
import python_ml
#import data_analysis
@@ -148,6 +151,35 @@ def upload_hdfs(local, hdfs, client):
for cols, corr_val in correlations.items():
    print("La corrélation de Pearson entre {} et {} est : {}".format(cols[0], cols[1], corr_val))

# Métriques prometheus

# Créer une métrique personnalisée
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
CPU_USAGE = Gauge('cpu_usage_percent', 'Pourcentage utilisation CPU')
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Utilisation de la mémoire')


# Fonction pour traiter une demande et mesurer le temps
def process_request():
    start_time = time.time()
    
    time.sleep(2)
    end_time = time.time()
    REQUEST_TIME.observe(end_time - start_time)

    cpu_percent = psutil.cpu_percent(interval=None) 
    memory_usage = psutil.virtual_memory().used 

    CPU_USAGE.set(cpu_percent)
    MEMORY_USAGE.set(memory_usage)

print('fin')
for local_csv in dfs:
    os.remove(local_csv)

if __name__ == '__main__':
    start_http_server(8004)
    process_request()