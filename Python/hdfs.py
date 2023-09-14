from hdfs import InsecureClient
#from pyspark.sql import SparkSession

hadoop_address = 'http://172.22.0.6:9870/'
client = InsecureClient(hadoop_address, user='root')
file = 'texte.txt'
client.makedirs('/projet')
hdfs_path = '/projet/' + file
client.upload(hdfs_path, file)