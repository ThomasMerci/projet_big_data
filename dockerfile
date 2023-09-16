FROM python:3.8

RUN apt-get update && apt-get install -y default-jre
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
WORKDIR /big_data/

RUN /usr/local/bin/python -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN python -c "import pyspark"

COPY /Python/python_hdfs.py ./python_hdfs.py
COPY /Python/texte.csv ./texte.csv

CMD ["python", "python_hdfs.py"]
