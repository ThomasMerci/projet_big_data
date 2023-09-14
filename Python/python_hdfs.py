from hdfs import InsecureClient
#from pyspark.sql import SparkSession

hadoop_address = 'http://172.24.0.6:9870/'
client = InsecureClient(hadoop_address, user='root')
fichier = 'texte.txt'
client.makedirs('/projet')
hdfs_path = '/projet/' + fichier
client.download(hdfs_path, fichier, overwrite=True)
print('ok')


#client.download(hdfs_path, fichier)
with open(fichier, 'r') as f:
    print("Contenu :")
    read = f.read()
    print(read)

