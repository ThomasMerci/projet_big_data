FROM python:3.6-slim 

WORKDIR /big_data/

COPY requirements.txt ./

RUN pip install -r requirements.txt
CMD ["python","./Python/hdfs.py"]

ENTRYPOINT python ./Python/hdfs.py