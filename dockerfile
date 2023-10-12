FROM openjdk:8-jdk

RUN apt-get update && apt-get install -y python3.9 python3-pip

#java
RUN JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::") && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export JAVA_HOME" >> /etc/profile
ENV JAVA_OPTS "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"
RUN echo $JAVA_HOME

#python
WORKDIR /big_data/
RUN python3.9 -m pip install --upgrade pip
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

#csv
COPY Python/texte.csv ./texte.csv
COPY data/bikes.csv ./bikes.csv
COPY data/bikeshops.csv ./bikeshops.csv
COPY data/orders.csv ./orders.csv
COPY data/customers.csv ./customers.csv

COPY /Python/python_extract.py ./python_extract.py
COPY /Python/python_ml.py ./python_ml.py
COPY /Python/python_deltalake.py ./python_deltalake.py
CMD ["/opt/spark/bin/spark-submit", "/big_data/python_deltalake.py"]