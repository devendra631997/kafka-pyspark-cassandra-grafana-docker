FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -y default-jre procps


RUN pip install --no-cache-dir -r requirements.txt

ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/default-java
# ENV INPUT_TOPIC='test_topic'
# ENV OUTPUT_TOPIC='output_topic_sink'
# ENV KAFKA_BOOTSTRAP_SERVERS='broker:29092'

COPY pyspark_consumer_1.py .

CMD ["python", "pyspark_consumer_1.py"]
