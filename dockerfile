FROM openjdk:8-jdk

RUN apt-get update && apt-get install -y python3.9 python3-pip

RUN JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export JAVA_HOME" >> /etc/profile
ENV JAVA_OPTS "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"
RUN echo $JAVA_HOME

WORKDIR /big_data/
RUN python3.9 -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY /Python/python_hdfs.py ./python_hdfs.py
COPY /Python/texte.csv ./texte.csv

CMD ["python3.9", "python_hdfs.py"]

