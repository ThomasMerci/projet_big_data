# projet_big_data

databricks:
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/7526033782065205/2846754932110018/7851752974584711/latest.html


![Alt text](diagr.png)


![Alt text](image.png)

docker tags:

kafka : docker pull bitnami/kafka:3.4.1-debian-11-r95
Nifi : docker pull apache/nifi:1.23.2
Spark : docker pull spark:3.4.1-python3
Delta Lake : docker pull deltaio/delta-docker:0.8.1_2.3.0
Prometheus : docker pull bitnami/prometheus:2.47.0
Grafana : docker pull grafana/grafana:9.4.14
Apache Ranger : docker pull apache/ranger:2.4.0
Hadoop : https://github.com/ThomasMerci/docker-hadoop


# lancement (dossier racine)

>docker network create projet_big_data_network

>bash_techno.bat ou bash_techno.sh


#important

Dans le nifi récupérer (clic droit) le templates du dossier racine (NIFIFLOW.xml)

Lancer les processors (en bas à gauche)


#lancement du projet avec spark

>bash.bat ou bash_projet.sh
