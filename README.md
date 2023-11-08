# projet_big_data

![Alt text](graphe.png)





# lancement (dossier racine)

>docker network create projet_big_data_network

>bash_techno.bat ou bash_techno.sh


#important

Dans le nifi récupérer (clic droit) le templates du dossier racine (NIFIFLOW.xml)

Lancer les processors (en bas à gauche)


#lancement du projet avec spark

>bash_projet.bat ou bash_projet.sh

# prometheus et Grafana 

>Prometheus : http://localhost:9090/ <br>
>Les jobs actifs sont dans targets <br>
>ils sont aussi accessible via ces adresses : <br>
>adresse_ip_du_container_prometheus:9090/metrics <br>
>adresse_ip_du_container_pushgateway:9091/metrics <br>
>adresse_ip_du_container_projet_big_data_python:8004/metrics <br>
>Grafana : http://localhost:3000/ <br>
>Il faut peut-être mettre en place un data source prometheus : <br>
>grafana -> config -> data source -> prometheus -> URL : http://prometheus:9090/ <br>
>Faire explore pour explorer les metriques <br>

Projet réalisé par Gaëtan ALLAH ASRA BADJINAN, Thomas MERCIER et Gabriello ZAFIFOMENDRAHA



