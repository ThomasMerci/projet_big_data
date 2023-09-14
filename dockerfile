FROM python:3.6-slim 

WORKDIR /big_data/

COPY requirements.txt ./
COPY /Python/python_hdfs.py ./python_hdfs.py
COPY /Python/texte.txt ./

RUN pip install -r requirements.txt

CMD ["python", "python_hdfs.py"]