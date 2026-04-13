from json import dumps
from kafka import KafkaProducer
import pandas as pd



kafka_nodes = "kafka:29092"
#myTopic = "sncbdata"
myTopic = "aisdata"

def gen_data():
  #df = pd.read_csv('input_sncb.csv')
  df = pd.read_csv('ais_instants.csv')

  prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))

  for index, row in df.iterrows():
    # following this structure : https://github.com/MobilityDB/MobilityNebula/blob/main/Queries/sncb_brake_monitoring.yaml
    #my_data = {
    #  't':        row[0],   # time_utc
    #  'deviceId': row[1],   # device_id
    #  'vbat':     row[2],   # vbat
    #  'pcfaMbar': row[3],   # PCFA_mbar
    #  'pcffMbar': row[4],   # PCFF_mbar
    #  'pcf1Mbar': row[5],   # PCF1_mbar
    #  'pcf2Mbar': row[6],   # PCF2_mbar
    #  't1mbar':   row[7],   # t1mbar
    #  't2mbar':   row[8],   # t2mbar
    #  'code1':    row[9],   # code1
    #  'code2':    row[10],  # code2
    #  'gpsSpeed': row[11],  # gps_speed
    #  'lat':      row[12],  # gps_lat
    #  'lon':      row[13],  # gps_lon
    #}
    my_data = {
      't': row['t'],
      'mmsi': row['mmsi'],
      'lon': row['longitude'],
      'lat': row['latitude'],
      'speed': row['sog'],
      'course': 0,
    }
    print(my_data)
    prod.send(topic=myTopic, value=my_data)

  prod.flush()

if __name__ == "__main__":
  gen_data()