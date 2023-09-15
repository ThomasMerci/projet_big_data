FROM python:3.6-slim 

RUN apt-get update && apt-get install -y openjdk-11-jre
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /big_data/

COPY requirements.txt ./
COPY /Python/python_hdfs.py ./python_hdfs.py
COPY /Python/texte.txt ./

RUN /usr/local/bin/python -m pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["python", "python_hdfs.py"]