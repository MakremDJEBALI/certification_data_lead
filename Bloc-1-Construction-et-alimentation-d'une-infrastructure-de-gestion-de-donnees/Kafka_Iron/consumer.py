# Example written based on the official 
# Confluent Kakfa Get started guide https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/consumer.py

from confluent_kafka import Consumer
import json
import ccloud_lib
import time
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv()
# %load_ext dotenv
# %dotenv

DBUSERNAME = os.getenv('DBUSERNAME')
DBPASSWORD = os.getenv('DBPASSWORD')
DBHOSTNAME = os.getenv('DBHOSTNAME')
DBNAME = os.getenv('DBNAME')

from sqlalchemy import create_engine, text
engine = create_engine(f"postgresql+psycopg2://{DBUSERNAME}:{DBPASSWORD}@{DBHOSTNAME}/{DBNAME}", echo=True)

# from sqlalchemy.orm import sessionmaker 
# Session = sessionmaker(bind=engine)
# session = Session()




# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python.config")
TOPIC = "iron_matches" 

# Create Consumer instance
# 'auto.offset.reset=earliest' to start reading from the beginning of the
# topic if no committed offsets exist
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'my_weather_consumer'
consumer_conf['auto.offset.reset'] = 'earliest' # This means that you will consume latest messages that your script haven't consumed yet!
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC])

# Process messages
try:
    while True:
        msg_list = consumer.consume(1, 1.0) # Search for all non-consumed events. It times out after 1 second
        if len(msg_list) == 0:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        # elif msg.error():
        #     print('error: {}'.format(msg.error()))
        else:
            data = {
            'championName': [],
            'individualPosition': [],
            'win': [],
            'kills': [],
            'deaths': [],
            'assists': [],
            'totalDamageDealtToChampions': [],
            'goldPerMinute': [],
            'champLevel': [],
            'laneMinionsFirst10Minutes': [],
            'jungleCsBefore10Minutes': [],
            'outerTurretExecutesBefore10Minutes': [],
            'visionScore': [],
            'visionScoreAdvantageLaneOpponent': [],
            'pentaKills': [],
            'perfectDragonSoulsTaken': [],
            'gameVersion': [],
            'gameId': []
            }

            # Check for Kafka message
            for msg in msg_list:
                record_key = msg.key()
                record_value = msg.value()
                data_from_producer = json.loads(record_value)
                data["championName"].append(data_from_producer["champion"])
                data["individualPosition"].append(data_from_producer["position"])
                data["win"].append(data_from_producer["win"])
                data["kills"].append(data_from_producer["kills"])
                data["deaths"].append(data_from_producer["deaths"])
                data["assists"].append(data_from_producer["assists"])
                data["totalDamageDealtToChampions"].append(data_from_producer["totalDamageDealtToChampions"])
                data["goldPerMinute"].append(data_from_producer["goldPerMinute"])
                data["champLevel"].append(data_from_producer["champLevel"])
                data["laneMinionsFirst10Minutes"].append(data_from_producer["laneMinionsFirst10Minutes"])
                data["jungleCsBefore10Minutes"].append(data_from_producer["jungleCsBefore10Minutes"])
                data["outerTurretExecutesBefore10Minutes"].append(data_from_producer["outerTurretExecutesBefore10Minutes"])
                data["visionScore"].append(data_from_producer["visionScore"])
                data["visionScoreAdvantageLaneOpponent"].append(data_from_producer["visionScoreAdvantageLaneOpponent"])
                data["pentaKills"].append(data_from_producer["pentaKills"])
                data["perfectDragonSoulsTaken"].append(data_from_producer["perfectDragonSoulsTaken"])
                data["gameVersion"].append(data_from_producer["gameVersion"])
                data["gameId"].append(data_from_producer["gameId"])

            df_iron = pd.DataFrame.from_dict(data)
            df_iron.to_sql("df_iron", 
                                engine,
                                if_exists = 'append')
                                
            print ("New df successfully sent to Database")

            # time.sleep(0.5) # Wait half a second

except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()



