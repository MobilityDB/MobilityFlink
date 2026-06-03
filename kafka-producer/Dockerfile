From python:3.8-slim

COPY requirements.txt .

RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt

# Copy resources
WORKDIR /
COPY wait-for-it.sh wait-for-it.sh 
#COPY aisdk_20110301_2000.csv .
#COPY multi_ship_trajectory.csv .
#COPY large_dataset.csv .
COPY ais_instants.csv .
COPY input_sncb.csv .
ADD python-producer.py .

CMD ./wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python -u python-producer.py